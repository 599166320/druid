package org.apache.druid.query.aggregation;

import org.apache.druid.query.core.DataPoint;
import org.apache.druid.query.core.TSG;
import org.apache.druid.segment.ColumnValueSelector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Iterator;

public class GorillaTsgMergeAggregator extends  BaseGorillaTscAggregator<ColumnValueSelector>{


    public GorillaTsgMergeAggregator(ColumnValueSelector columnValueSelector) {
        super(columnValueSelector);
    }

    @Override
    void bufferAdd(ByteBuffer buf) {

    }

    @Override
    public void aggregate() {
        Object obj = selector.getObject();
        if (obj == null) {
            return;
        }
        if(obj instanceof TSG){
            TSG other = (TSG) obj;
            Iterator<DataPoint> tsgIterator =other.toIterator();
            while(tsgIterator.hasNext()){
                tsg.put(tsgIterator.next());
            }
        }
    }

    @Nullable
    @Override
    public Object get() {
        return null;
    }

    @Override
    public void close() {

    }
}
