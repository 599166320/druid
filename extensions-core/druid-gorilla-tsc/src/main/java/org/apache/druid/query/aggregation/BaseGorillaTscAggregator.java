package org.apache.druid.query.aggregation;
import org.apache.druid.query.core.DataPoint;
import org.apache.druid.query.core.TSG;
import org.apache.druid.query.core.TSGIterator;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.TreeMap;

public abstract class BaseGorillaTscAggregator <TSelector>
        implements  Aggregator,BufferAggregator{
    protected final TSelector selector;
    protected TSG tsg;
    private final int maxNumEntries;
    private boolean onHeap;
    public BaseGorillaTscAggregator(TSelector selector,int maxNumEntries, boolean onHeap){
        this.selector = selector;
        this.maxNumEntries = maxNumEntries;
        this.onHeap = onHeap;
    }

    @Override
    public float getFloat() {
        throw new UnsupportedOperationException("BaseGorillaTscAggregator does not support getFloat()");
    }

    @Override
    public long getLong() {
        throw new UnsupportedOperationException("BaseGorillaTscAggregator does not support getLong()");
    }

    @Override
    public void init(ByteBuffer buf, int position) {
    }

    @Override
    public void aggregate(ByteBuffer buf, int position) {
    }

    @Override
    public void aggregate() {
    }

    @Nullable
    @Override
    public Object get(ByteBuffer buf, int position) {
        return null;
    }

    @Nullable
    @Override
    public Object get() {
        return null;
    }

    @Override
    public float getFloat(ByteBuffer buf, int position) {
        return 0;
    }

    @Override
    public long getLong(ByteBuffer buf, int position) {
        return 0;
    }
    protected   TSG addNewPoint(Object[][] obj,TSG tsg) {
        //添加新的点
        Object[][] timeAndValues = obj;
        for (Object[] timeAndValue : timeAndValues) {
            if(tsg.getTime() >(Long) timeAndValue[0]){
                //需要重新构建tsg
                TreeMap<Long,Double> treeMap = new TreeMap<>();
                TSGIterator tsgIterator = tsg.toIterator();
                while(tsgIterator.hasNext()){
                    DataPoint dataPoint = tsgIterator.next();
                    treeMap.put(dataPoint.getTime(),dataPoint.getValue());
                }
                treeMap.put((Long) timeAndValue[0], (Double) timeAndValue[1]);
                tsg = new TSG(treeMap.firstKey()-treeMap.firstKey()%3600);
                for(Map.Entry<Long,Double> e:treeMap.entrySet()){
                    tsg.put(e.getKey(),e.getValue());
                }
            }else{
                tsg.put((Long) timeAndValue[0], (Double) timeAndValue[1]);
            }
        }
        return tsg;
    }
}
