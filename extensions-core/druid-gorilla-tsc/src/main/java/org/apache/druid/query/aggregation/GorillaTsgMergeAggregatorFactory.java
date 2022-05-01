package org.apache.druid.query.aggregation;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.segment.BaseNullableColumnValueSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.NilColumnValueSelector;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public class GorillaTsgMergeAggregatorFactory extends GorillaTscAggregatorFactory{
    private final String fieldName;

    public GorillaTsgMergeAggregatorFactory(String name, String field, @Nullable Integer maxIntermediateSize) {
        super(name, field, maxIntermediateSize);
        this.fieldName = field;
    }
    @Override
    public Aggregator factorize(final ColumnSelectorFactory metricFactory)
    {
        return makeMergeAggregator(metricFactory,true);
    }

    @Override
    public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory) {
        return makeMergeAggregator(metricFactory,false);
    }

    @Override
    public byte[] getCacheKey() {
        return new CacheKeyBuilder(AggregatorUtil.GORILLA_MERGE_CACHE_TYPE_ID)
                .appendString(fieldName)
                .appendInt(DEFAULT_MAX_INTERMEDIATE_SIZE)
                .build();
    }

    private BaseGorillaTscAggregator makeMergeAggregator(ColumnSelectorFactory columnFactory,boolean onHeap)
    {
        final BaseNullableColumnValueSelector selector = columnFactory.makeColumnValueSelector(fieldName);
        if (selector instanceof NilColumnValueSelector) {
            throw new ISE("Unexpected NilColumnValueSelector");
        }
        return super.getBaseGorillaTscAggregator(columnFactory,onHeap);
    }
}
