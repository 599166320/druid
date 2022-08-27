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

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.druid.collections.MultiColumnSorter;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.BaseSequence;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryTimeoutException;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.segment.*;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.filter.Filters;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.Interval;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.stream.Collectors;

public class ScanQueryEngine
{
  static final String LEGACY_TIMESTAMP_KEY = "timestamp";

  public Sequence<ScanResultValue> process(
      final ScanQuery query,
      final Segment segment,
      final ResponseContext responseContext
  )
  {
    // "legacy" should be non-null due to toolChest.mergeResults
    final boolean legacy = Preconditions.checkNotNull(query.isLegacy(), "Expected non-null 'legacy' parameter");

    final Object numScannedRows = responseContext.get(ResponseContext.Key.NUM_SCANNED_ROWS);
    if (numScannedRows != null) {
      long count = (long) numScannedRows;
      if (count >= query.getScanRowsLimit() && query.getOrder().equals(ScanQuery.Order.NONE)) {
        return Sequences.empty();
      }
    }
    final boolean hasTimeout = QueryContexts.hasTimeout(query);
    final long timeoutAt = (long) responseContext.get(ResponseContext.Key.TIMEOUT_AT);
    final long start = System.currentTimeMillis();
    final StorageAdapter adapter = segment.asStorageAdapter();

    if (adapter == null) {
      throw new ISE(
          "Null storage adapter found. Probably trying to issue a query against a segment being memory unmapped."
      );
    }

    final List<String> allColumns = new ArrayList<>();

    if (query.getColumns() != null && !query.getColumns().isEmpty()) {
      if (legacy && !query.getColumns().contains(LEGACY_TIMESTAMP_KEY)) {
        allColumns.add(LEGACY_TIMESTAMP_KEY);
      }

      // Unless we're in legacy mode, allColumns equals query.getColumns() exactly. This is nice since it makes
      // the compactedList form easier to use.
      allColumns.addAll(query.getColumns());
    } else {
      final Set<String> availableColumns = Sets.newLinkedHashSet(
          Iterables.concat(
              Collections.singleton(legacy ? LEGACY_TIMESTAMP_KEY : ColumnHolder.TIME_COLUMN_NAME),
              Iterables.transform(
                  Arrays.asList(query.getVirtualColumns().getVirtualColumns()),
                  VirtualColumn::getOutputName
              ),
              adapter.getAvailableDimensions(),
              adapter.getAvailableMetrics()
          )
      );

      allColumns.addAll(availableColumns);

      if (legacy) {
        allColumns.remove(ColumnHolder.TIME_COLUMN_NAME);
      }
    }

    final List<Interval> intervals = query.getQuerySegmentSpec().getIntervals();
    Preconditions.checkArgument(intervals.size() == 1, "Can only handle a single interval, got[%s]", intervals);

    final SegmentId segmentId = segment.getId();

    final Filter filter = Filters.convertToCNFFromQueryContext(query, Filters.toFilter(query.getFilter()));

    responseContext.add(ResponseContext.Key.NUM_SCANNED_ROWS, 0L);
    final long limit = calculateRemainingScanRowsLimit(query, responseContext);
    if(query.getOrderByColumns().size() > 0 && query.getContext().containsKey("orderByDirection")){
      return getScanOrderByResultValueSequence(query, responseContext, legacy, hasTimeout, timeoutAt, start, adapter, allColumns, intervals, segmentId, filter);
    }
    return getScanResultValueSequence(query, responseContext, legacy, hasTimeout, timeoutAt, start, adapter, allColumns, intervals, segmentId, filter, limit);
  }
  private Sequence<ScanResultValue> getScanOrderByResultValueSequence(ScanQuery query, ResponseContext responseContext, boolean legacy, boolean hasTimeout, long timeoutAt, long start, StorageAdapter adapter, List<String> allColumns, List<Interval> intervals, SegmentId segmentId, Filter filter) {

    List<String> sortColumns = query.getOrderByColumns();
    List<String> orderByDirection = (List<String>) query.getContext().get("orderByDirection");
    final int limit  = (int)query.getScanRowsLimit();
    Comparator<MultiColumnSorter.MultiColumnSorterElement<Long>> comparator = new Comparator<MultiColumnSorter.MultiColumnSorterElement<Long>>() {
      @Override
      public int compare(MultiColumnSorter.MultiColumnSorterElement<Long> o1, MultiColumnSorter.MultiColumnSorterElement<Long> o2) {
        for(int i = 0; i < o1.getOrderByColumValues().size() ; i++){
          if(!o1.getOrderByColumValues().get(i).equals(o2.getOrderByColumValues().get(i))){
            if(ScanQuery.Order.ASCENDING.equals(ScanQuery.Order.fromString(orderByDirection.get(i)))){
              return o1.getOrderByColumValues().get(i).compareTo(o2.getOrderByColumValues().get(i));
            }else{
              return o2.getOrderByColumValues().get(i).compareTo(o1.getOrderByColumValues().get(i));
            }
          }
        }
        return 0;
      }
    };
    MultiColumnSorter<Long> multiColumnSorter = new MultiColumnSorter<Long>(limit,comparator);

    Sequence<Cursor> cursorSequence = adapter.makeCursors(filter, intervals.get(0), query.getVirtualColumns(), Granularities.ALL, query.getOrder().equals(ScanQuery.Order.DESCENDING) || (query.getOrder().equals(ScanQuery.Order.NONE) && query.isDescending()), null);
    cursorSequence.toList().stream().map(cursor -> new BaseSequence<>(
            new BaseSequence.IteratorMaker<ScanResultValue, Iterator<ScanResultValue>>()
            {
              @Override
              public Iterator<ScanResultValue> make()
              {
                final List<BaseObjectColumnValueSelector> columnSelectors = new ArrayList<>(sortColumns.size());

                for (String column : sortColumns) {
                  final BaseObjectColumnValueSelector selector;

                  if (legacy && LEGACY_TIMESTAMP_KEY.equals(column)) {
                    selector = cursor.getColumnSelectorFactory()
                            .makeColumnValueSelector(ColumnHolder.TIME_COLUMN_NAME);
                  } else {
                    selector = cursor.getColumnSelectorFactory().makeColumnValueSelector(column);
                  }

                  columnSelectors.add(selector);
                }

                return new Iterator<ScanResultValue>()
                {
                  private long offset = 0;

                  @Override
                  public boolean hasNext()
                  {
                    return !cursor.isDone();
                  }

                  @Override
                  public ScanResultValue next()
                  {
                    if (!hasNext()) {
                      throw new NoSuchElementException();
                    }
                    if (hasTimeout && System.currentTimeMillis() >= timeoutAt) {
                      throw new QueryTimeoutException(StringUtils.nonStrictFormat("Query [%s] timed out", query.getId()));
                    }
                    final long lastOffset = offset;
                    this.rowsToCompactedList();
                    responseContext.add(ResponseContext.Key.NUM_SCANNED_ROWS, offset - lastOffset);
                    if (hasTimeout) {
                      responseContext.put(
                              ResponseContext.Key.TIMEOUT_AT,
                              timeoutAt - (System.currentTimeMillis() - start)
                      );
                    }
                    return new ScanResultValue(segmentId.toString(), allColumns, multiColumnSorter);
                  }

                  @Override
                  public void remove()
                  {
                    throw new UnsupportedOperationException();
                  }

                  private void rowsToCompactedList()
                  {
                    while(!cursor.isDone()) {
                      List<Comparable> sortValues = sortColumns.stream().map(c->(Comparable)getColumnValue(sortColumns.indexOf(c))).collect(Collectors.toList());
                      multiColumnSorter.add(this.offset,sortValues);
                      cursor.advance();
                      ++this.offset;
                    }
                  }

                  private Object getColumnValue(int i)
                  {
                    final BaseObjectColumnValueSelector selector = columnSelectors.get(i);
                    final Object value;

                    if (legacy && allColumns.get(i).equals(LEGACY_TIMESTAMP_KEY)) {
                      value = DateTimes.utc((long) selector.getObject());
                    } else {
                      value = selector == null ? null : selector.getObject();
                    }
                    return value;
                  }
                };
              }

              @Override
              public void cleanup(Iterator<ScanResultValue> iterFromMake)
              {
              }
            }
    )).forEach((s) -> {
      s.toList();
    });

    final Set<Long> topKOffset = new HashSet<>(limit);
    Iterators.addAll(topKOffset, multiColumnSorter.drain());
    return Sequences.concat(
            adapter
                    .makeCursors(
                            filter,
                            intervals.get(0),
                            query.getVirtualColumns(),
                            Granularities.ALL,
                            query.getOrder().equals(ScanQuery.Order.DESCENDING) ||
                                    (query.getOrder().equals(ScanQuery.Order.NONE) && query.isDescending()),
                            null
                    )
                    .map(cursor -> new BaseSequence<>(
                            new BaseSequence.IteratorMaker<ScanResultValue, Iterator<ScanResultValue>>()
                            {
                              @Override
                              public Iterator<ScanResultValue> make()
                              {
                                final List<BaseObjectColumnValueSelector> columnSelectors = new ArrayList<>(allColumns.size());

                                for (String column : allColumns) {
                                  final BaseObjectColumnValueSelector selector;

                                  if (legacy && LEGACY_TIMESTAMP_KEY.equals(column)) {
                                    selector = cursor.getColumnSelectorFactory()
                                            .makeColumnValueSelector(ColumnHolder.TIME_COLUMN_NAME);
                                  } else {
                                    selector = cursor.getColumnSelectorFactory().makeColumnValueSelector(column);
                                  }

                                  columnSelectors.add(selector);
                                }

                                final int batchSize = query.getBatchSize();
                                return new Iterator<ScanResultValue>()
                                {
                                  private long offset = 0;

                                  @Override
                                  public boolean hasNext()
                                  {
                                    return !cursor.isDone() && offset < limit;
                                  }

                                  @Override
                                  public ScanResultValue next()
                                  {
                                    if (!hasNext()) {
                                      throw new NoSuchElementException();
                                    }
                                    if (hasTimeout && System.currentTimeMillis() >= timeoutAt) {
                                      throw new QueryTimeoutException(StringUtils.nonStrictFormat("Query [%s] timed out", query.getId()));
                                    }
                                    final long lastOffset = offset;
                                    final Object events;
                                    final ScanQuery.ResultFormat resultFormat = query.getResultFormat();
                                    if (ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST.equals(resultFormat)) {
                                      events = rowsToCompactedList();
                                    } else if (ScanQuery.ResultFormat.RESULT_FORMAT_LIST.equals(resultFormat)) {
                                      events = rowsToList();
                                    } else {
                                      throw new UOE("resultFormat[%s] is not supported", resultFormat.toString());
                                    }
                                    responseContext.add(ResponseContext.Key.NUM_SCANNED_ROWS, offset - lastOffset);
                                    if (hasTimeout) {
                                      responseContext.put(
                                              ResponseContext.Key.TIMEOUT_AT,
                                              timeoutAt - (System.currentTimeMillis() - start)
                                      );
                                    }
                                    return new ScanResultValue(segmentId.toString(), allColumns, events);
                                  }

                                  @Override
                                  public void remove()
                                  {
                                    throw new UnsupportedOperationException();
                                  }

                                  private List<List<Object>> rowsToCompactedList()
                                  {
                                    final List<List<Object>> events = new ArrayList<>(batchSize);

                                    if(topKOffset.size() > 0){
                                      for (; !cursor.isDone(); cursor.advance(), offset++) {
                                        if (topKOffset.contains(this.offset)) {
                                          final List<Object> theEvent = new ArrayList<>(allColumns.size());
                                          for (int j = 0; j < allColumns.size(); j++) {
                                            theEvent.add(getColumnValue(j));
                                          }
                                          events.add(theEvent);
                                        }
                                      }
                                    }else {
                                      final long iterLimit = Math.min(limit, offset + batchSize);
                                      for (; !cursor.isDone() && offset < iterLimit; cursor.advance(), offset++) {
                                        final List<Object> theEvent = new ArrayList<>(allColumns.size());
                                        for (int j = 0; j < allColumns.size(); j++) {
                                          theEvent.add(getColumnValue(j));
                                        }
                                        events.add(theEvent);
                                      }
                                    }
                                    return events;
                                  }

                                  private List<Map<String, Object>> rowsToList()
                                  {
                                    List<Map<String, Object>> events = Lists.newArrayListWithCapacity(batchSize);
                                    if (topKOffset.size() > 0) {
                                      for (; !cursor.isDone(); cursor.advance(), offset++) {
                                        if (topKOffset.contains(this.offset)) {
                                          final Map<String, Object> theEvent = new LinkedHashMap<>();
                                          for (int j = 0; j < allColumns.size(); j++) {
                                            theEvent.put(allColumns.get(j), getColumnValue(j));
                                          }
                                          events.add(theEvent);
                                        }
                                      }
                                    }else {
                                      final long iterLimit = Math.min(limit, offset + batchSize);
                                      for (; !cursor.isDone() && offset < iterLimit; cursor.advance(), offset++) {
                                        final Map<String, Object> theEvent = new LinkedHashMap<>();
                                        for (int j = 0; j < allColumns.size(); j++) {
                                          theEvent.put(allColumns.get(j), getColumnValue(j));
                                        }
                                        events.add(theEvent);
                                      }
                                    }
                                    return events;
                                  }

                                  private Object getColumnValue(int i)
                                  {
                                    final BaseObjectColumnValueSelector selector = columnSelectors.get(i);
                                    final Object value;

                                    if (legacy && allColumns.get(i).equals(LEGACY_TIMESTAMP_KEY)) {
                                      value = DateTimes.utc((long) selector.getObject());
                                    } else {
                                      value = selector == null ? null : selector.getObject();
                                    }

                                    return value;
                                  }
                                };
                              }

                              @Override
                              public void cleanup(Iterator<ScanResultValue> iterFromMake)
                              {
                              }
                            }
                    ))
    );
  }
  @Nonnull
  private Sequence<ScanResultValue> getScanResultValueSequence(ScanQuery query, ResponseContext responseContext, boolean legacy, boolean hasTimeout, long timeoutAt, long start, StorageAdapter adapter, List<String> allColumns, List<Interval> intervals, SegmentId segmentId, Filter filter, long limit) {
    return Sequences.concat(
            adapter
                .makeCursors(
                        filter,
                    intervals.get(0),
                    query.getVirtualColumns(),
                    Granularities.ALL,
                    query.getOrder().equals(ScanQuery.Order.DESCENDING) ||
                    (query.getOrder().equals(ScanQuery.Order.NONE) && query.isDescending()),
                    null
                )
                .map(cursor -> new BaseSequence<>(
                    new BaseSequence.IteratorMaker<ScanResultValue, Iterator<ScanResultValue>>()
                    {
                      @Override
                      public Iterator<ScanResultValue> make()
                      {
                        final List<BaseObjectColumnValueSelector> columnSelectors = new ArrayList<>(allColumns.size());

                        for (String column : allColumns) {
                          final BaseObjectColumnValueSelector selector;

                          if (legacy && LEGACY_TIMESTAMP_KEY.equals(column)) {
                            selector = cursor.getColumnSelectorFactory()
                                             .makeColumnValueSelector(ColumnHolder.TIME_COLUMN_NAME);
                          } else {
                            selector = cursor.getColumnSelectorFactory().makeColumnValueSelector(column);
                          }

                          columnSelectors.add(selector);
                        }

                        final int batchSize = query.getBatchSize();
                        return new Iterator<ScanResultValue>()
                        {
                          private long offset = 0;

                          @Override
                          public boolean hasNext()
                          {
                            return !cursor.isDone() && offset < limit;
                          }

                          @Override
                          public ScanResultValue next()
                          {
                            if (!hasNext()) {
                              throw new NoSuchElementException();
                            }
                            if (hasTimeout && System.currentTimeMillis() >= timeoutAt) {
                              throw new QueryTimeoutException(StringUtils.nonStrictFormat("Query [%s] timed out", query.getId()));
                            }
                            final long lastOffset = offset;
                            final Object events;
                            final ScanQuery.ResultFormat resultFormat = query.getResultFormat();
                            if (ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST.equals(resultFormat)) {
                              events = rowsToCompactedList();
                            } else if (ScanQuery.ResultFormat.RESULT_FORMAT_LIST.equals(resultFormat)) {
                              events = rowsToList();
                            } else {
                              throw new UOE("resultFormat[%s] is not supported", resultFormat.toString());
                            }
                            responseContext.add(ResponseContext.Key.NUM_SCANNED_ROWS, offset - lastOffset);
                            if (hasTimeout) {
                              responseContext.put(
                                  ResponseContext.Key.TIMEOUT_AT,
                                  timeoutAt - (System.currentTimeMillis() - start)
                              );
                            }
                            return new ScanResultValue(segmentId.toString(), allColumns, events);
                          }

                          @Override
                          public void remove()
                          {
                            throw new UnsupportedOperationException();
                          }

                          private List<List<Object>> rowsToCompactedList()
                          {
                            final List<List<Object>> events = new ArrayList<>(batchSize);
                            final long iterLimit = Math.min(limit, offset + batchSize);
                            for (; !cursor.isDone() && offset < iterLimit; cursor.advance(), offset++) {
                              final List<Object> theEvent = new ArrayList<>(allColumns.size());
                              for (int j = 0; j < allColumns.size(); j++) {
                                theEvent.add(getColumnValue(j));
                              }
                              events.add(theEvent);
                            }
                            return events;
                          }

                          private List<Map<String, Object>> rowsToList()
                          {
                            List<Map<String, Object>> events = Lists.newArrayListWithCapacity(batchSize);
                            final long iterLimit = Math.min(limit, offset + batchSize);
                            for (; !cursor.isDone() && offset < iterLimit; cursor.advance(), offset++) {
                              final Map<String, Object> theEvent = new LinkedHashMap<>();
                              for (int j = 0; j < allColumns.size(); j++) {
                                theEvent.put(allColumns.get(j), getColumnValue(j));
                              }
                              events.add(theEvent);
                            }
                            return events;
                          }

                          private Object getColumnValue(int i)
                          {
                            final BaseObjectColumnValueSelector selector = columnSelectors.get(i);
                            final Object value;

                            if (legacy && allColumns.get(i).equals(LEGACY_TIMESTAMP_KEY)) {
                              value = DateTimes.utc((long) selector.getObject());
                            } else {
                              value = selector == null ? null : selector.getObject();
                            }

                            return value;
                          }
                        };
                      }

                      @Override
                      public void cleanup(Iterator<ScanResultValue> iterFromMake)
                      {
                      }
                    }
            ))
    );
  }

  /**
   * If we're performing time-ordering, we want to scan through the first `limit` rows in each segment ignoring the number
   * of rows already counted on other segments.
   */
  private long calculateRemainingScanRowsLimit(ScanQuery query, ResponseContext responseContext)
  {
    if (query.getOrder().equals(ScanQuery.Order.NONE)) {
      return query.getScanRowsLimit() - (long) responseContext.get(ResponseContext.Key.NUM_SCANNED_ROWS);
    }
    return query.getScanRowsLimit();
  }
}
