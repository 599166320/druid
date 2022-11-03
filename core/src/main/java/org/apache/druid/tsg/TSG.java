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

import org.apache.druid.java.util.common.StringUtils;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import static java.util.Objects.requireNonNull;

public class TSG
{
  private final long startTime;
  private final OutBit outBit;
  private long time;
  private double value;
  private int timeDelta;
  private int leading;
  private int trailing;
  private boolean closed;

  /**
   * Create new TSG block for given start time.
   *
   * @param startTime block start time (epoch)
   * @throws IllegalArgumentException if given start time is below zero
   */
  public TSG(final long startTime)
  {
    this(startTime, new OutBitSet());
  }

  /**
   * Create new TSG block for given start time and OutBit storage.
   * Use this constructor in case of custom OutBit implementations.
   *
   * @param startTime block start time
   * @param outBit    output bit storage
   * @throws IllegalArgumentException if given start time is below zero
   * @throws NullPointerException     for null OutBit
   */
  public TSG(
      final long startTime,
      final OutBit outBit
  )
  {
    if (startTime < 0) {
      throw new IllegalArgumentException(
          String.format("Invalid time value `%d`.", startTime));
    }
    this.startTime = startTime;
    this.outBit = requireNonNull(outBit);
  }

  /**
   * Create TSG block from dump (bytes).
   *
   * @param bytes TSG dump in bytes
   * @return TSG block created from given bytes
   * @throws BufferOverflowException for invalid bytes provided
   */
  public static TSG fromBytes(final byte[] bytes)
  {
    return fromBytes(ByteBuffer.wrap(bytes));
  }

  public static TSG fromBytes(final ByteBuffer byteBuffer)
  {
    final long startTime = byteBuffer.getLong();
    final long time = byteBuffer.getLong();
    final double value = byteBuffer.getDouble();
    final int timeDelta = byteBuffer.getInt();
    final int leading = byteBuffer.getInt();
    final int trailing = byteBuffer.getInt();
    final int position = byteBuffer.getInt();
    byte[] bytesArray = new byte[byteBuffer.remaining()];
    byteBuffer.get(bytesArray, 0, bytesArray.length);
    final TSG tsg = new TSG(
        startTime,
        new OutBitSet(BitSet.valueOf(bytesArray), position)
    );
    tsg.time = time;
    tsg.value = value;
    tsg.timeDelta = timeDelta;
    tsg.leading = leading;
    tsg.trailing = trailing;
    return tsg;
  }

  public static byte[] merge(byte[] lhsBytes, byte[] rhsBytes)
  {

    TSG lhs = TSG.fromBytes(lhsBytes);
    TSG rhs = TSG.fromBytes(rhsBytes);

    return merge(lhs, rhs).toBytes();
  }

  /**
   * 相同tag的时间序列合并
   *
   * @param lhs
   * @param rhs
   * @return
   */
  public static TSG merge(TSG lhs, TSG rhs)
  {
    TreeMap<Long, Double> treeMap = new TreeMap<>();
    Iterator<DataPoint> tsgIterator = lhs.toIterator();
    while (tsgIterator.hasNext()) {
      DataPoint dataPoint = tsgIterator.next();
      treeMap.put(dataPoint.getTime(), dataPoint.getValue());
    }
    tsgIterator = rhs.toIterator();
    while (tsgIterator.hasNext()) {
      DataPoint dataPoint = tsgIterator.next();
      treeMap.put(dataPoint.getTime(), dataPoint.getValue());
    }
    TSG tsg = new TSG(treeMap.firstKey());
    for (Map.Entry<Long, Double> e : treeMap.entrySet()) {
      tsg.put(e.getKey(), e.getValue());
    }
    return tsg;
  }


  /**
   * Close current TSG block.
   * After this point no more points are accepted,
   * and bytes dump can be done via the <code>TSG::getDataBytes</code> method only.
   */
  public synchronized void close()
  {
    if (!closed) {
      outBit.flipBits(36);
      outBit.skipBit();
      closed = true;
    }
  }

  /**
   * Append <code>DataPoint</code> to current block.
   *
   * @param dataPoint value in time container
   * @throws IllegalArgumentException if given time is misplaced for TSG start time or last inserted time
   */
  public synchronized void put(final DataPoint dataPoint)
  {
    put(dataPoint.getTime(), dataPoint.getValue());
  }

  /**
   * Append time and value to current block.
   *
   * @param time  given time
   * @param value given value
   * @throws IllegalArgumentException if given time is misplaced for TSG start time or last inserted time
   */
  public synchronized void put(
      final long time,
      final double value
  )
  {
    if (closed) {
      throw new IllegalStateException("Block already closed.");
    }
    if (this.time == 0) {
      if (time < this.startTime) {
        throw new IllegalArgumentException(
            String.format("Issued time: `%d` is out of block start time: `%d`.", time, startTime));
      }
      putInitialPoint(time, value);
    } else {
      if (time < this.time) {
        throw new IllegalArgumentException(
            String.format("Issued time: `%d` is before last inserted: `%d`.", time, this.time));
      }
      putTime(time);
      putValue(value);
    }
  }

  /**
   * Check if the current block is closed.
   *
   * @return indication whether given block was closed
   */
  public synchronized boolean isClosed()
  {
    return closed;
  }

  /**
   * Create the current TSG block dump in open state.
   * Closed block cannot be exported with state,
   * only by its content by the <code>TSG::getDataBytes</code> method.
   *
   * @return TSG dump
   * @throws IllegalStateException if block was already closed
   */
  public synchronized byte[] toBytes()
  {
    if (closed) {
      throw new IllegalStateException("Block already closed, dump data instead.");
    }
    return ByteUtils.concat(
        ByteBuffer.allocate(40)
                  .putLong(startTime)
                  .putLong(time)
                  .putDouble(value)
                  .putInt(timeDelta)
                  .putInt(leading)
                  .putInt(trailing)
                  .putInt(outBit.getSize())
                  .array(),
        outBit.toBytes()
    );
  }

  /**
   * Create current TSG data dump.
   * This method can be called on closed block only.
   * Open block sne
   *
   * @return TSG data dump
   * @throws IllegalStateException if block was not yet closed
   */
  public synchronized byte[] getDataBytes()
  {
    if (!closed) {
      throw new IllegalStateException("Block not sealed yet.");
    }
    return outBit.toBytes();
  }

  /**
   * Create Iterator of current TSG block.
   * Created Iterator operates on copied OutBit bits.
   * Iterator can be created from both opened and closed block of TSG.
   *
   * @return Iterator of current TSG block
   */
  public synchronized TSGIterator toIterator()
  {
    if (closed) {
      return new TSGIterator(new InBitSet(outBit.copy().toBytes()));
    } else {
      final OutBit outBitCopy = outBit.copy();
      outBitCopy.flipBits(36);
      outBitCopy.skipBit();
      return new TSGIterator(new InBitSet(outBitCopy.toBytes()));
    }
  }

  private void putInitialPoint(
      final long time,
      final double value
  )
  {
    outBit.writeLong(startTime);
    this.time = time;
    this.value = value;
    timeDelta = (int) (time - startTime);
    outBit.write(timeDelta, 14);
    outBit.writeLong(Double.doubleToLongBits(value));
  }

  private void putTime(final long time)
  {
    int timeDelta = (int) (time - this.time);
    int timeDeltaDelta = timeDelta - this.timeDelta;
    if (timeDeltaDelta == 0) {
      outBit.skipBit();
    } else if (-63 <= timeDeltaDelta && timeDeltaDelta <= 64) {
      outBit.flipBit();
      outBit.skipBit();
      outBit.write(timeDeltaDelta, 7);
    } else if (-255 <= timeDeltaDelta && timeDeltaDelta <= 256) {
      outBit.flipBits(2);
      outBit.skipBit();
      outBit.write(timeDeltaDelta, 9);
    } else if (-2047 <= timeDeltaDelta && timeDeltaDelta <= 2048) {
      outBit.flipBits(3);
      outBit.skipBit();
      outBit.write(timeDeltaDelta, 12);
    } else {
      outBit.flipBits(4);
      outBit.write(timeDeltaDelta, 32);
    }
    this.timeDelta = timeDelta;
    this.time = time;
  }

  private void putValue(final double value)
  {
    final long valueDelta = Double.doubleToLongBits(value) ^ Double.doubleToLongBits(this.value);
    if (valueDelta == 0) {
      outBit.skipBit();
    } else {
      outBit.flipBit();
      final int valueLeadingZeros = Math.min(Long.numberOfLeadingZeros(valueDelta), 31);
      final int valueTrailingZeros = Long.numberOfTrailingZeros(valueDelta);
      if (this.leading != 31 &&
          valueLeadingZeros >= this.leading &&
          valueTrailingZeros >= this.trailing) {
        outBit.skipBit();
        outBit.write(
            valueDelta >> this.trailing,
            64 - this.leading - this.trailing
        );
      } else {
        leading = valueLeadingZeros;
        trailing = valueTrailingZeros;
        outBit.flipBit();
        outBit.write(leading, 5);
        int significantBits = 64 - leading - trailing;
        outBit.write(significantBits, 6);
        outBit.write(valueDelta >> trailing, significantBits);
      }
    }
    this.value = value;
  }

  public long getStartTime()
  {
    return startTime;
  }

  public long getTime()
  {
    return time;
  }

  public static int compare(TSG a, TSG b)
  {
    return Long.compare(a.startTime, b.startTime);
  }

  public OutBit getOutBitSet()
  {
    return outBit;
  }

  @Override
  public String toString()
  {
    return StringUtils.encodeBase64String(this.toBytes());
  }

  public TreeMap<Long, Double> toTreeMap(long start, long end)
  {
    TreeMap<Long, Double> treeMap = new TreeMap<>();
    TSGIterator iterator = this.toIterator();
    while (iterator.hasNext()) {
      DataPoint dataPoint = iterator.next();
      if (dataPoint.getTime() >= start && dataPoint.getTime() <= end) {
        treeMap.put(dataPoint.getTime(), dataPoint.getValue());
      }
    }
    return treeMap;
  }

  public TreeMap<Long, Double> toTreeMap()
  {
    TreeMap<Long, Double> treeMap = new TreeMap<>();
    TSGIterator iterator = this.toIterator();
    while (iterator.hasNext()) {
      DataPoint dataPoint = iterator.next();
      treeMap.put(dataPoint.getTime(), dataPoint.getValue());
    }
    return treeMap;
  }

  public static TSG fromTreeMap(TreeMap<Long, Double> treeMap)
  {
    TSG tsg = new TSG(treeMap.firstKey());
    for (Map.Entry<Long, Double> e : treeMap.entrySet()) {
      tsg.put(e.getKey(), e.getValue());
    }
    return tsg;
  }

  public static TreeMap<Long, Double> getTimeAndValues(String base64)
  {
    return fromBytes(StringUtils.decodeBase64String(base64)).toTreeMap();
  }
}
