package org.apache.druid.query.scan;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.groupby.orderby.OrderByColumnSpec;

import java.util.*;

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
        final long limit  = query.getScanRowsLimit();
        TreeMap<Object, Object> sortValueAndScanResultValue = new TreeMap();
        List<String> sortColumns = query.getOrderByColumns();
        List<String> orderByDirection = (List<String>) query.getContext().get("orderByDirection");
        List<String> columns = new ArrayList<>();
        while (!yielder.isDone()) {
            ScanResultValue srv = yielder.get();
            // Only replace once using the columns from the first event
            columns = columns.isEmpty() ? srv.getColumns() : columns;
            int idx = columns.indexOf(sortColumns.get(0));
            List events = (List)(srv.getEvents());
            for(Object event:events){
                sortValueAndScanResultValue.put(((List)event).get(idx),event);
                // Finish scanning the interval containing the limit row
                if (sortValueAndScanResultValue.size() > limit) {
                    if (OrderByColumnSpec.Direction.fromString(orderByDirection.get(0)) == OrderByColumnSpec.Direction.DESCENDING) {
                        sortValueAndScanResultValue.remove(sortValueAndScanResultValue.firstKey());
                    }else {
                        sortValueAndScanResultValue.remove(sortValueAndScanResultValue.lastKey());
                    }
                }
            }

            yielder = yielder.next(null);
            count++;
        }
        return new ScanResultValue(null, columns, new ArrayList<>(sortValueAndScanResultValue.values()));
    }
}
