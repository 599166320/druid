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
package org.apache.druid.query.aggregation;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.guice.GorillaTscSerializersModule;
import org.apache.druid.query.core.DataPoint;
import org.apache.druid.query.core.TSG;
import org.apache.druid.segment.GenericColumnSerializer;
import org.apache.druid.segment.column.ColumnBuilder;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.ObjectStrategy;
import org.apache.druid.segment.serde.ComplexColumnPartSupplier;
import org.apache.druid.segment.serde.ComplexMetricExtractor;
import org.apache.druid.segment.serde.ComplexMetricSerde;
import org.apache.druid.segment.serde.LargeColumnSupportedComplexColumnSerializer;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
public class GorillaTscComplexMetricSerde extends ComplexMetricSerde
{
  private static final GorillaTscHolderObjectStrategy STRATEGY = new GorillaTscHolderObjectStrategy();
  @Override
  public String getTypeName()
  {
    return GorillaTscSerializersModule.GORILLA_TSC_TYPE_NAME;
  }

  @Override
  public ComplexMetricExtractor getExtractor()
  {
    return new ComplexMetricExtractor(){
      @Override
      public Class extractedClass() {
        return TSG.class;
      }
      @Nullable
      @Override
      public Object extractValue(InputRow inputRow, String metricName) {
        throw new UnsupportedOperationException("extractValue without an aggregator factory is not supported.");
      }

      @Nullable
      @Override
      public Object extractValue(InputRow inputRow, String metricName, AggregatorFactory agg) {
        //传递初始化的参数
        GorillaTscAggregatorFactory gorillaTscAggregatorFactory = (GorillaTscAggregatorFactory) agg;
        Object rawValue = inputRow.getRaw(metricName);
        if (rawValue instanceof TSG) {
          return (TSG) rawValue;
        } else {
          List<String> dimValues = inputRow.getDimension(metricName);
          if (dimValues == null) {
            return null;
          }
          Object[][] datas = new Object[dimValues.size()][2];
          for(int i=0;i<datas.length;i++){
            datas[i][0] = inputRow.getTimestampFromEpoch()/1000;
            datas[i][1] = Double.valueOf(dimValues.get(i));
          }
          return datas;
        }
      }
    };
  }

  @Override
  public void deserializeColumn(ByteBuffer buffer, ColumnBuilder columnBuilder)
  {
    final GenericIndexed<TSG> column = GenericIndexed.read(
            buffer,
            this.getObjectStrategy(),
            columnBuilder.getFileMapper()
    );
    columnBuilder.setComplexColumnSupplier(new ComplexColumnPartSupplier(getTypeName(), column));
  }

  @Override
  public GenericColumnSerializer getSerializer(SegmentWriteOutMedium segmentWriteOutMedium, String column)
  {
    return LargeColumnSupportedComplexColumnSerializer.create(
            segmentWriteOutMedium,
            column,
            this.getObjectStrategy()
    );
  }

  @Override
  public ObjectStrategy<TSG> getObjectStrategy(){
    return STRATEGY;
  }
}
