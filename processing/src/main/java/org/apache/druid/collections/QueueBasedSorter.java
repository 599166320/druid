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

package org.apache.druid.collections;

import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.Ordering;
import org.apache.druid.java.util.common.ISE;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

/**
 * This sorter is applicable to two cases:
 * 1.Result Set Merge
 * 2.Sort the internal data of segment in the way of delayed materialization
 */
public class QueueBasedSorter<T> implements Sorter<T>
{

  private final MinMaxPriorityQueue<List<T>> queue;

  public QueueBasedSorter(int limit, Comparator<List<T>> comparator)
  {
    this.queue = MinMaxPriorityQueue
        .orderedBy(Ordering.from(comparator))
        .maximumSize(limit)
        .create();
  }

  public QueueBasedSorter(int limit, Ordering<List<T>> ordering)
  {
    this.queue = MinMaxPriorityQueue
        .orderedBy(ordering)
        .maximumSize(limit)
        .create();
  }

  @Override
  public void add(List<T> sorterElement)
  {
    try {
      queue.offer(sorterElement);
    }
    catch (ClassCastException e) {
      throw new ISE("The sorted column cannot have different types of values.");
    }
  }

  @Override
  public Iterator<List<T>> drainElement()
  {
    return new Iterator<List<T>>()
    {
      @Override
      public boolean hasNext()
      {
        return !queue.isEmpty();
      }

      @Override
      public List<T> next()
      {
        return queue.poll();
      }

    };
  }

  @Override
  public int size()
  {
    return queue.size();
  }
}
