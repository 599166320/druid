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

package org.apache.druid.tsg;

import java.util.Objects;

public class DefaultDataPoint implements DataPoint
{
  private final long time;
  private final double value;

  public DefaultDataPoint(
      final long time,
      final double value
  )
  {
    if (time < 0) {
      throw new IllegalArgumentException(
          String.format("Invalid time value `%d`.", time));
    }
    this.time = time;
    this.value = value;
  }

  @Override
  public long getTime()
  {
    return time;
  }

  @Override
  public double getValue()
  {
    return value;
  }

  @Override
  public boolean equals(final Object o)
  {
      if (this == o) {
          return true;
      }
      if (o == null || getClass() != o.getClass()) {
          return false;
      }
    final DefaultDataPoint dataPoint = (DefaultDataPoint) o;
    return time == dataPoint.time &&
           Double.compare(dataPoint.value, value) == 0;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(time, value);
  }

  @Override
  public String toString()
  {
    return "DefaultDataPoint{" +
           "time=" + time +
           ", value=" + value +
           '}';
  }
}
