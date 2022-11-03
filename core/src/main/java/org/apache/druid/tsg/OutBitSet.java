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

public class OutBitSet implements OutBit
{
  private final BitSet bitSet;
  private int position = 0;

  public OutBitSet()
  {
    bitSet = new BitSet();
  }

  OutBitSet(
      final BitSet bitSet,
      final int position
  )
  {
    if (position < 0) {
      throw new IllegalArgumentException(
          String.format("Invalid position: `%d`.", position));
    }
    this.bitSet = requireNonNull(bitSet);
    this.position = position;
  }

  @Override
  public void skipBit()
  {
    ++position;
  }

  @Override
  public void flipBit()
  {
    bitSet.flip(position);
    ++position;
  }

  @Override
  public void flipBits(final int n)
  {
    bitSet.flip(position, position + n);
    position += n;
  }

  @Override
  public void write(final long value, final int size)
  {
    for (int i = 0; i < size; ++i) {
      if (ByteUtils.getBit(value, i)) {
        bitSet.set(position);
      }
      ++position;
    }
  }

  @Override
  public int getSize()
  {
    return position;
  }

  @Override
  public byte[] toBytes()
  {
    return bitSet.toByteArray();
  }

  @Override
  public OutBit copy()
  {
    return new OutBitSet((BitSet) bitSet.clone(), position);
  }
}
