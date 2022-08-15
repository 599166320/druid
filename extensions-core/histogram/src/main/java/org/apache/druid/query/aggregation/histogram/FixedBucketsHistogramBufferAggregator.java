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

package org.apache.druid.query.aggregation.histogram;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.BaseObjectColumnValueSelector;

import java.nio.ByteBuffer;
import java.util.IdentityHashMap;

public class FixedBucketsHistogramBufferAggregator implements BufferAggregator
{
  private final BaseObjectColumnValueSelector selector;
  private final IdentityHashMap<ByteBuffer, Int2ObjectMap<FixedBucketsHistogram>> cache = new IdentityHashMap();
  private final double lowerLimit;
  private final double upperLimit;
  private final int numBuckets;
  private final FixedBucketsHistogram.OutlierHandlingMode outlierHandlingMode;

  public FixedBucketsHistogramBufferAggregator(
      BaseObjectColumnValueSelector selector,
      double lowerLimit,
      double upperLimit,
      int numBuckets,
      FixedBucketsHistogram.OutlierHandlingMode outlierHandlingMode
  )
  {
    this.selector = selector;
    this.lowerLimit = lowerLimit;
    this.upperLimit = upperLimit;
    this.numBuckets = numBuckets;
    this.outlierHandlingMode = outlierHandlingMode;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    FixedBucketsHistogram values = new FixedBucketsHistogram(
            lowerLimit,
            upperLimit,
            numBuckets,
            outlierHandlingMode
    );
    addToCache(buf,position,values);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    Object val = selector.getObject();
    Int2ObjectMap<FixedBucketsHistogram> int2ObjectMap = cache.get(buf);
    FixedBucketsHistogram values  = int2ObjectMap.get(position);
    values.combine(val);
  }

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    FixedBucketsHistogram tmp = cache.get(buf).get(position);
    return tmp;
  }

  @Override
  public float getFloat(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("FixedBucketsHistogramBufferAggregator does not support getFloat()");
  }

  @Override
  public long getLong(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("FixedBucketsHistogramBufferAggregator does not support getLong()");
  }

  @Override
  public double getDouble(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("FixedBucketsHistogramBufferAggregator does not support getDouble()");
  }

  @Override
  public void close()
  {
    cache.clear();
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    inspector.visit("selector", selector);
  }
  private void addToCache(final ByteBuffer buffer, final int position, final FixedBucketsHistogram values)
  {
    Int2ObjectMap<FixedBucketsHistogram> map = cache.computeIfAbsent(buffer, b -> new Int2ObjectOpenHashMap<>());
    map.put(position, values);
  }

  @Override
  public void relocate(int oldPosition, int newPosition, ByteBuffer oldBuffer, ByteBuffer newBuffer) {
    FixedBucketsHistogram values = cache.get(oldBuffer).get(oldPosition);
    addToCache(newBuffer, newPosition, values);
    final Int2ObjectMap<FixedBucketsHistogram> map = cache.get(oldBuffer);
    map.remove(oldPosition);
    if (map.isEmpty()) {
      cache.remove(oldBuffer);
    }
  }
}
