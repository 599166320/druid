/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.server.coordinator.duty;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.collect.Lists;
import org.apache.druid.client.ImmutableDruidDataSource;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.metadata.MetadataRuleManager;
import org.apache.druid.server.coordinator.CoordinatorStats;
import org.apache.druid.server.coordinator.DruidCluster;
import org.apache.druid.server.coordinator.DruidCoordinator;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.ReplicationThrottler;
import org.apache.druid.server.coordinator.rules.BroadcastDistributionRule;
import org.apache.druid.server.coordinator.rules.DropRule;
import org.apache.druid.server.coordinator.rules.ForeverDropRule;
import org.apache.druid.server.coordinator.rules.Rule;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.DateTime;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 */
public class RunRules implements CoordinatorDuty
{
  private static final EmittingLogger log = new EmittingLogger(RunRules.class);
  private static final int MAX_MISSING_RULES = 10;

  public static final Cache<String, AtomicLong> MAX_DROP_RULE_CACHE = Caffeine.newBuilder().initialCapacity(5).build();

  public static final AtomicLong DEFAULT_DROP_NUM = new AtomicLong(0);

  private final ReplicationThrottler replicatorThrottler;

  private final DruidCoordinator coordinator;

  public RunRules(DruidCoordinator coordinator)
  {
    this(
        new ReplicationThrottler(
            coordinator.getDynamicConfigs().getReplicationThrottleLimit(),
            coordinator.getDynamicConfigs().getReplicantLifetime(),
            false
        ),
        coordinator
    );
  }

  public RunRules(ReplicationThrottler replicatorThrottler, DruidCoordinator coordinator)
  {
    this.replicatorThrottler = replicatorThrottler;
    this.coordinator = coordinator;
  }

  @Override
  public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
  {
    replicatorThrottler.updateParams(
        coordinator.getDynamicConfigs().getReplicationThrottleLimit(),
        coordinator.getDynamicConfigs().getReplicantLifetime(),
        false
    );

    CoordinatorStats stats = new CoordinatorStats();
    DruidCluster cluster = params.getDruidCluster();

    if (cluster.isEmpty()) {
      log.warn("Uh... I have no servers. Not assigning anything...");
      return params;
    }

    // Get used segments which are overshadowed by other used segments. Those would not need to be loaded and
    // eventually will be unloaded from Historical servers. Segments overshadowed by *served* used segments are marked
    // as unused in MarkAsUnusedOvershadowedSegments, and then eventually Coordinator sends commands to Historical nodes
    // to unload such segments in UnloadUnusedSegments.
    Set<SegmentId> overshadowed = params.getDataSourcesSnapshot().getOvershadowedSegments();

    for (String tier : cluster.getTierNames()) {
      replicatorThrottler.updateReplicationState(tier);
    }

    DruidCoordinatorRuntimeParams paramsWithReplicationManager = params
        .buildFromExistingWithoutSegmentsMetadata()
        .withReplicationManager(replicatorThrottler)
        .build();

    // Run through all matched rules for used segments
    DateTime now = DateTimes.nowUtc();
    MetadataRuleManager databaseRuleManager = paramsWithReplicationManager.getDatabaseRuleManager();

    final List<SegmentId> segmentsWithMissingRules = Lists.newArrayListWithCapacity(MAX_MISSING_RULES);
    int missingRules = 0;

    final Set<String> broadcastDatasources = new HashSet<>();
    for (ImmutableDruidDataSource dataSource : params.getDataSourcesSnapshot().getDataSourcesMap().values()) {
      List<Rule> rules = databaseRuleManager.getRulesWithDefault(dataSource.getName());
      for (Rule rule : rules) {
        // A datasource is considered a broadcast datasource if it has any broadcast rules.
        // The set of broadcast datasources is used by BalanceSegments, so it's important that RunRules
        // executes before BalanceSegments.
        if (rule instanceof BroadcastDistributionRule) {
          broadcastDatasources.add(dataSource.getName());
          break;
        }
      }
    }

    for (DataSegment segment : params.getUsedSegments()) {
      if (overshadowed.contains(segment.getId())) {
        // Skipping overshadowed segments
        continue;
      }
      List<Rule> rules = databaseRuleManager.getRulesWithDefault(segment.getDataSource());
      boolean foundMatchingRule = false;
      final int ruleSize = rules.size();
      for (int i = 0; i < ruleSize; i++) {
        if (rules.get(i).appliesTo(segment, now)) {
          if (
              stats.getGlobalStat(
                  "totalNonPrimaryReplicantsLoaded") >= paramsWithReplicationManager.getCoordinatorDynamicConfig()
                                                                                    .getMaxNonPrimaryReplicantsToLoad()
              && !paramsWithReplicationManager.getReplicationManager().isLoadPrimaryReplicantsOnly()
          ) {
            log.info(
                "Maximum number of non-primary replicants [%d] have been loaded for the current RunRules execution. Only loading primary replicants from here on for this coordinator run cycle.",
                paramsWithReplicationManager.getCoordinatorDynamicConfig().getMaxNonPrimaryReplicantsToLoad()
            );
            paramsWithReplicationManager.getReplicationManager().setLoadPrimaryReplicantsOnly(true);
          }
          stats.accumulate(rules.get(i).run(coordinator, paramsWithReplicationManager, segment));
          foundMatchingRule = true;
          break;
        } else {
          if (ruleSize == i + 1 && rules.get(i + 1) instanceof ForeverDropRule) {
            //倒数第二个规则是loadRule,最后一个是dropRule
            continue;
          } else if (MAX_DROP_RULE_CACHE.get(segment.getDataSource(),o-> DEFAULT_DROP_NUM).decrementAndGet() > 0) {
            //只是删除本层次的副本而已，不会修改used字段
            rules.get(i).dropAllExpireSegments(paramsWithReplicationManager, segment);
          }
        }
      }

      if (!foundMatchingRule) {
        if (segmentsWithMissingRules.size() < MAX_MISSING_RULES) {
          segmentsWithMissingRules.add(segment.getId());
        }
        missingRules++;
      }
    }

    if (!segmentsWithMissingRules.isEmpty()) {
      log.makeAlert("Unable to find matching rules!")
         .addData("segmentsWithMissingRulesCount", missingRules)
         .addData("segmentsWithMissingRules", segmentsWithMissingRules)
         .emit();
    }

    return params.buildFromExisting()
                 .withCoordinatorStats(stats)
                 .withBroadcastDatasources(broadcastDatasources)
                 .build();
  }
}
