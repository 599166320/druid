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

import java.util.BitSet;

import static java.util.Objects.requireNonNull;

public class InBitSet implements InBit
{
  private int position;
  private final BitSet bitSet;

  public InBitSet(final byte[] bytes)
  {
    this(BitSet.valueOf(bytes));
  }

  public InBitSet(final BitSet bitSet)
  {
    this.bitSet = requireNonNull(bitSet);
  }

  @Override
  public boolean read()
  {
    final boolean current = bitSet.get(position);
    ++position;
    return current;
  }

  @Override
  public long read(final int size)
  {
    if (size > 64) {
      throw new IllegalArgumentException(
          String.format("Over long read: `%d`.", size));
    }
    long value = 0L;
    for (int i = 0; i < size; ++i) {
      value += bitSet.get(position) ? (1L << i) : 0L;
      ++position;
    }
    return value;
  }
}
