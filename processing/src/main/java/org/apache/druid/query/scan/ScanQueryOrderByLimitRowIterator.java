package org.apache.druid.query.scan;
import com.google.common.collect.Iterators;
import org.apache.druid.collections.MultiColumnSorter;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.context.ResponseContext;
import java.util.*;
import java.util.stream.Collectors;

public class ScanQueryOrderByLimitRowIterator extends ScanQueryLimitRowIterator{

    public ScanQueryOrderByLimitRowIterator(QueryRunner<ScanResultValue> baseRunner, QueryPlus<ScanResultValue> queryPlus, ResponseContext responseContext) {
        super(baseRunner, queryPlus, responseContext);
    }

    @Override
    public boolean hasNext() {
        return !yielder.isDone();
    }

    @Override
    public ScanResultValue next() {
        if (ScanQuery.ResultFormat.RESULT_FORMAT_VALUE_VECTOR.equals(resultFormat)) {
            throw new UOE(ScanQuery.ResultFormat.RESULT_FORMAT_VALUE_VECTOR + " is not supported yet");
        }
        final int limit  = (int)query.getScanRowsLimit();
        List<String> sortColumns = query.getOrderByColumns();
        List<String> orderByDirection = (List<String>) query.getContext().get("orderByDirection");
        Comparator<MultiColumnSorter.MultiColumnSorterElement<Object>> comparator = new Comparator<MultiColumnSorter.MultiColumnSorterElement<Object>>() {
            @Override
            public int compare(MultiColumnSorter.MultiColumnSorterElement<Object> o1, MultiColumnSorter.MultiColumnSorterElement<Object> o2) {
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
        MultiColumnSorter<Object> multiColumnSorter = new MultiColumnSorter<Object>(limit,comparator);

        List<String> columns = new ArrayList<>();
        while (!yielder.isDone()) {
            ScanResultValue srv = yielder.get();
            // Only replace once using the columns from the first event
            columns = columns.isEmpty() ? srv.getColumns() : columns;
            List<Integer> idxs = sortColumns.stream().map(c->srv.getColumns().indexOf(c)).collect(Collectors.toList());
            List events = (List)(srv.getEvents());
            for(Object event:events){
                List<Comparable> sortValues = idxs.stream().map(idx->((List<Comparable>)event).get(idx)).collect(Collectors.toList());
                multiColumnSorter.add(event,sortValues);
            }

            yielder = yielder.next(null);
            count++;
        }
        final List<Object> sortedElements = new ArrayList<>(limit);
        Iterators.addAll(sortedElements, multiColumnSorter.drain());
        return new ScanResultValue(null, columns, sortedElements);
    }
}
