package org.apache.druid.query.aggregation.quantile;

import org.apache.druid.query.aggregation.BufferAggregator;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public class NoopValueAppendBufferAggregator implements BufferAggregator {


    private static final NoopValueAppendBufferAggregator INSTANCE = new NoopValueAppendBufferAggregator();

    static NoopValueAppendBufferAggregator instance()
    {
        return INSTANCE;
    }

    @Override
    public void init(ByteBuffer buf, int position) {

    }

    @Override
    public void aggregate(ByteBuffer buf, int position) {

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

    @Override
    public void close() {

    }
}
