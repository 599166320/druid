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

package org.apache.druid.segment.loading;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.ReferenceCountingSegment;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.SegmentLazyLoadFailCallback;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.Interval;
import org.junit.Test;

/**
*/
public class CacheTestSegmentLoader implements SegmentLoader
{

  @Override
  public ReferenceCountingSegment getSegment(final DataSegment segment, boolean lazy, SegmentLazyLoadFailCallback SegmentLazyLoadFailCallback)
  {
    Segment baseSegment = new Segment()
    {
      @Override
      public SegmentId getId()
      {
        return segment.getId();
      }

      @Override
      public Interval getDataInterval()
      {
        return segment.getInterval();
      }

      @Override
      public QueryableIndex asQueryableIndex()
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public StorageAdapter asStorageAdapter()
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public void close()
      {
      }
    };
    return ReferenceCountingSegment.wrapSegment(baseSegment, segment.getShardSpec());
  }

  @Override
  public void cleanup(DataSegment segment)
  {

  }

  @Test
  public void test(){
    try {
      JsonMapper jsonMapper = new JsonMapper();
      String payLoad = "{\"dataSource\":\"tracing_downstream_rt_agg\",\"interval\":\"2022-03-06T04:00:00.000Z/2022-03-06T05:00:00.000Z\",\"version\":\"2022-03-07T04:32:59.939Z\",\"loadSpec\":{\"type\":\"hdfs\",\"path\":\"hdfs://opsdevcluster/druid/segments/tracing_downstream_rt_agg/20220306T040000.000Z_20220306T050000.000Z/2022-03-07T04_32_59.939Z/0_index.zip\"},\"dimensions\":\"a_api,a_ip,a_serviceName,b_api,b_ip,b_serviceName,b_sql,a_idcId,b_idcId\",\"metrics\":\"a_duration,a_error,a_reqs,a_slowRequest,qStr\",\"shardSpec\":{\"type\":\"numbered\",\"partitionNum\":0,\"partitions\":1},\"lastCompactionState\":{\"partitionsSpec\":{\"type\":\"dynamic\",\"maxRowsPerSegment\":5000000,\"maxTotalRows\":9223372036854775807},\"indexSpec\":{\"bitmap\":{\"type\":\"roaring\",\"compressRunOnSerialization\":true},\"dimensionCompression\":\"lz4\",\"metricCompression\":\"lz4\",\"longEncoding\":\"longs\",\"segmentLoader\":null},\"granularitySpec\":{\"type\":\"uniform\",\"segmentGranularity\":\"HOUR\",\"queryGranularity\":\"MINUTE\",\"rollup\":true,\"intervals\":[\"2022-03-06T04:00:00.000Z/2022-03-06T05:00:00.000Z\"]}},\"binaryVersion\":9,\"size\":155840547,\"identifier\":\"tracing_downstream_rt_agg_2022-03-06T04:00:00.000Z_2022-03-06T05:00:00.000Z_2022-03-07T04:32:59.939Z\"}";
      DataSegment segment = jsonMapper.readValue(payLoad, DataSegment.class);
      System.out.println(segment);
    }catch (Exception e){
      e.printStackTrace();
    }
  }
}
