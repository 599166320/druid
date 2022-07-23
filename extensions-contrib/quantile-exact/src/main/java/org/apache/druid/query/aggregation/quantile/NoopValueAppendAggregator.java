package org.apache.druid.query.aggregation.quantile;

import org.apache.druid.query.aggregation.Aggregator;

public class NoopValueAppendAggregator implements Aggregator
{
    public NoopValueAppendAggregator()
    {
    }

    @Override
    public void aggregate()
    {
    }

    @Override
    public Object get()
    {
        return 0L;
    }

    @Override
    public float getFloat()
    {
        return 0.0f;
    }

    @Override
    public long getLong()
    {
        return 0L;
    }

    @Override
    public double getDouble()
    {
        return 0.0;
    }

    @Override
    public void close()
    {
    }
}