package org.apache.druid.query.aggregation;

public class GorillaTscQueryComplexMetricSerde extends GorillaTscComplexMetricSerde{
    @Override
    public String getTypeName()
    {
        return GorillaTscQueryAggregatorFactory.TYPE_NAME;
    }
}
