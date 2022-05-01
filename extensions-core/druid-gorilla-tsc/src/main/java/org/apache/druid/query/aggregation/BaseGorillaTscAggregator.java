package org.apache.druid.query.aggregation;
import org.apache.druid.query.core.TSG;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;
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
}
