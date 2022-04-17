package org.apache.druid.query.aggregation;
import org.apache.druid.query.core.TSG;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
public abstract class BaseGorillaTscAggregator <TSelector>
        implements  Aggregator,BufferAggregator{
    protected final TSelector selector;
    protected TSG tsg;
    public BaseGorillaTscAggregator(TSelector selector){
        this.selector = selector;
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
        System.out.println(buf);
    }

    @Override
    public void aggregate(ByteBuffer buf, int position) {
        final int oldPosition = buf.position();
        try {
            buf.position(position);
            bufferAdd(buf);
        }
        finally {
            buf.position(oldPosition);
        }
    }

    @Nullable
    @Override
    public Object get(ByteBuffer buf, int position) {
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

    abstract void bufferAdd(ByteBuffer buf);
}
