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

package org.apache.druid.indexing.kafka;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.apache.druid.data.input.kafka.KafkaRecordEntity;
import org.apache.druid.indexing.appenderator.ActionBasedSegmentAllocator;
import org.apache.druid.indexing.appenderator.ActionBasedUsedSegmentChecker;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.SegmentAllocateAction;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTask;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskRunner;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.realtime.FireDepartmentMetrics;
import org.apache.druid.segment.realtime.appenderator.Appenderator;
import org.apache.druid.segment.realtime.appenderator.StreamAppenderatorDriver;
import org.apache.druid.timeline.partition.KafkaPartitionBasedNumberedPartialShardSpec;

import java.util.*;

public class KafkaIndexTask extends SeekableStreamIndexTask<Integer, Long, KafkaRecordEntity>
{
  private static final String TYPE = "index_kafka";

  private final KafkaIndexTaskIOConfig ioConfig;
  private final ObjectMapper configMapper;

  // This value can be tuned in some tests
  private long pollRetryMs = 30000;

  @JsonCreator
  public KafkaIndexTask(
      @JsonProperty("id") String id,
      @JsonProperty("resource") TaskResource taskResource,
      @JsonProperty("dataSchema") DataSchema dataSchema,
      @JsonProperty("tuningConfig") KafkaIndexTaskTuningConfig tuningConfig,
      @JsonProperty("ioConfig") KafkaIndexTaskIOConfig ioConfig,
      @JsonProperty("context") Map<String, Object> context,
      @JacksonInject ObjectMapper configMapper
  )
  {
    super(
        getOrMakeId(id, dataSchema.getDataSource(), TYPE),
        taskResource,
        dataSchema,
        tuningConfig,
        ioConfig,
        context,
        getFormattedGroupId(dataSchema.getDataSource(), TYPE)
    );
    this.configMapper = configMapper;
    this.ioConfig = ioConfig;

    Preconditions.checkArgument(
        ioConfig.getStartSequenceNumbers().getExclusivePartitions().isEmpty(),
        "All startSequenceNumbers must be inclusive"
    );
  }

  long getPollRetryMs()
  {
    return pollRetryMs;
  }

  @Override
  protected SeekableStreamIndexTaskRunner<Integer, Long, KafkaRecordEntity> createTaskRunner()
  {
    //noinspection unchecked
    return new IncrementalPublishingKafkaIndexTaskRunner(
        this,
        dataSchema.getParser(),
        authorizerMapper,
        lockGranularityToUse
    );
  }

  @Override
  protected KafkaRecordSupplier newTaskRecordSupplier()
  {
    ClassLoader currCtxCl = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

      final Map<String, Object> props = new HashMap<>(((KafkaIndexTaskIOConfig) super.ioConfig).getConsumerProperties());

      props.put("auto.offset.reset", "none");

      return new KafkaRecordSupplier(props, configMapper);
    }
    finally {
      Thread.currentThread().setContextClassLoader(currCtxCl);
    }
  }

  @Override
  @JsonProperty
  public KafkaIndexTaskTuningConfig getTuningConfig()
  {
    return (KafkaIndexTaskTuningConfig) super.getTuningConfig();
  }

  @VisibleForTesting
  void setPollRetryMs(long retryMs)
  {
    this.pollRetryMs = retryMs;
  }

  @Override
  @JsonProperty("ioConfig")
  public KafkaIndexTaskIOConfig getIOConfig()
  {
    return (KafkaIndexTaskIOConfig) super.getIOConfig();
  }

  @Override
  public String getType()
  {
    return TYPE;
  }

  @Override
  public boolean supportsQueries()
  {
    return true;
  }

  @Override
  public StreamAppenderatorDriver newDriver(
          final Appenderator appenderator,
          final TaskToolbox toolbox,
          final FireDepartmentMetrics metrics,
          final Set<Integer> partitionIdTypeSet
  )
  {
    if (!this.getIOConfig().getConsumerProperties().containsKey("kafkaTotalPartition")
            || !this.getIOConfig().getConsumerProperties().containsKey("partitionDimensions")
            || !this.getIOConfig().getConsumerProperties().containsKey("partitionFunction"))
    {
      return super.newDriver(appenderator,toolbox,metrics,partitionIdTypeSet);
    }
    Integer kafkaTotalPartition = (Integer) this.getIOConfig().getConsumerProperties().get("kafkaTotalPartition");
    String partitionDimensions = (String) this.getIOConfig().getConsumerProperties().get("partitionDimensions");
    String partitionFunction = (String) this.getIOConfig().getConsumerProperties().get("partitionFunction");
    return new StreamAppenderatorDriver(
            appenderator,
            new ActionBasedSegmentAllocator(
                    toolbox.getTaskActionClient(),
                    dataSchema,
                    (schema, row, sequenceName, previousSegmentId, skipSegmentLineageCheck) -> new SegmentAllocateAction(
                            schema.getDataSource(),
                            row.getTimestamp(),
                            schema.getGranularitySpec().getQueryGranularity(),
                            schema.getGranularitySpec().getSegmentGranularity(),
                            sequenceName,
                            previousSegmentId,
                            skipSegmentLineageCheck,
                            new KafkaPartitionBasedNumberedPartialShardSpec(Arrays.asList(partitionDimensions.split(",")),partitionIdTypeSet, kafkaTotalPartition,partitionFunction),
                            lockGranularityToUse
                    )
            ),
            toolbox.getSegmentHandoffNotifierFactory(),
            new ActionBasedUsedSegmentChecker(toolbox.getTaskActionClient()),
            toolbox.getDataSegmentKiller(),
            toolbox.getJsonMapper(),
            metrics
    );
  }
}
