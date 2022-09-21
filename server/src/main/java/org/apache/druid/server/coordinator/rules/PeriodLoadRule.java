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

package org.apache.druid.server.coordinator.rules;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import org.apache.druid.client.DruidServer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.server.coordinator.CoordinatorStats;
import org.apache.druid.server.coordinator.DruidCluster;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.ServerHolder;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;

import java.util.Map;
import java.util.NavigableSet;

/**
 */
public class PeriodLoadRule extends LoadRule
{
  private static final EmittingLogger log = new EmittingLogger(PeriodLoadRule.class);
  static final boolean DEFAULT_INCLUDE_FUTURE = true;

  private final Period period;
  private final boolean includeFuture;
  private final Map<String, Integer> tieredReplicants;

  @JsonCreator
  public PeriodLoadRule(
      @JsonProperty("period") Period period,
      @JsonProperty("includeFuture") Boolean includeFuture,
      @JsonProperty("tieredReplicants") Map<String, Integer> tieredReplicants
  )
  {
    this.tieredReplicants = tieredReplicants == null ? ImmutableMap.of(DruidServer.DEFAULT_TIER, DruidServer.DEFAULT_NUM_REPLICANTS) : tieredReplicants;
    validateTieredReplicants(this.tieredReplicants);
    this.period = period;
    this.includeFuture = includeFuture == null ? DEFAULT_INCLUDE_FUTURE : includeFuture;
  }

  @Override
  @JsonProperty
  public String getType()
  {
    return "loadByPeriod";
  }

  @JsonProperty
  public Period getPeriod()
  {
    return period;
  }

  @JsonProperty
  public boolean isIncludeFuture()
  {
    return includeFuture;
  }

  @Override
  @JsonProperty
  public Map<String, Integer> getTieredReplicants()
  {
    return tieredReplicants;
  }

  @Override
  public int getNumReplicants(String tier)
  {
    final Integer retVal = tieredReplicants.get(tier);
    return retVal == null ? 0 : retVal;
  }

  @Override
  public boolean appliesTo(DataSegment segment, DateTime referenceTimestamp)
  {
    return appliesTo(segment.getInterval(), referenceTimestamp);
  }

  @Override
  public boolean appliesTo(Interval interval, DateTime referenceTimestamp)
  {
    return Rules.eligibleForLoad(period, interval, referenceTimestamp, includeFuture);
  }
  @Override
  public void dropAllExpireSegments(
      final DruidCoordinatorRuntimeParams params,
      final DataSegment segment
  )
  {
    try {
      targetReplicants.putAll(getTieredReplicants());
      currentReplicants.putAll(params.getSegmentReplicantLookup().getClusterTiers(segment.getId()));
      final CoordinatorStats stats = new CoordinatorStats();
      final DruidCluster druidCluster = params.getDruidCluster();
      //final boolean isLoading = loadingInProgress(druidCluster);//如何避免查询出现断点,比如热节点删除数据，冷节点还没有加载完
      //算法,1.冷节点已经加载完这个segment,才删除其他层的数据，这种是最完美的.2.延迟删除，比如丢在队列中，等一段时间再删除,4.使用beak决定rule是否终止。
      //遍历当前segmentId对应的副本列表[{tier1:num1},{tier2:num2}]
      for (final Object2IntMap.Entry<String> entry : currentReplicants.object2IntEntrySet()) {
        final String tier = entry.getKey();
        if(!targetReplicants.containsKey(tier) && targetReplicants.size() == 1){
          //目标规则没有匹配的tier
          continue;
        }
        final NavigableSet<ServerHolder> holders = druidCluster.getHistoricalsByTier(tier);
        final int numDropped;
        if (holders == null) {
          log.makeAlert("No holders found for tier[%s]", tier).emit();
          numDropped = 0;
        } else {
          final int currentReplicantsInTier = entry.getIntValue();
          if (currentReplicantsInTier > 0) {
            // This enforces that loading is completed before we attempt to drop stuffs as a safety measure.
            log.info(
                "drop segment [%s], from tier [%s] ,CurrentReplication=[%s],TargetReplication=[%s]",
                segment.getId(),
                tier,
                getCurrentReplicationLogString(),
                getTargetReplicationLogString()
            );

            numDropped = dropForTier(
                currentReplicantsInTier,
                holders,
                segment,
                params.getBalancerStrategy()
            );
          } else {
            numDropped = 0;
          }
        }
        stats.addToTieredStat(DROPPED_COUNT, tier, numDropped);
      }
    } finally {
      targetReplicants.clear();
      currentReplicants.clear();
    }
  }
}
