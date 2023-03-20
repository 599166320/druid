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

package org.apache.druid.query.scan;

import com.google.common.collect.Iterables;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryProcessingPool;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.context.ResponseContext;


public class ScanQueryOrderByRunner implements QueryRunner<ScanResultValue>
{

  private final QueryProcessingPool queryProcessingPool;
  private final Iterable<QueryRunner<ScanResultValue>> queryables;

  public ScanQueryOrderByRunner(
      QueryProcessingPool queryProcessingPool,
      Iterable<QueryRunner<ScanResultValue>> queryables
  )
  {
    this.queryProcessingPool = queryProcessingPool;
    this.queryables = Iterables.unmodifiableIterable(queryables);
  }

  @Override
  public Sequence<ScanResultValue> run(QueryPlus<ScanResultValue> queryPlus, ResponseContext responseContext)
  {
    return new ScanQueryOrderBySequence(queryPlus, queryProcessingPool, queryables, responseContext);
  }
}