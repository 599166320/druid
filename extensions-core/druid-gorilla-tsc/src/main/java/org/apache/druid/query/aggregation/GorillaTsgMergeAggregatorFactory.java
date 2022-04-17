package org.apache.druid.query.aggregation;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.segment.BaseNullableColumnValueSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.NilColumnValueSelector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ValueType;
import javax.annotation.Nullable;

public class GorillaTsgMergeAggregatorFactory extends GorillaTscAggregatorFactory{
    private final String fieldName;

    public GorillaTsgMergeAggregatorFactory(String name, String field, @Nullable Integer maxIntermediateSize) {
        super(name, field, maxIntermediateSize);
        this.fieldName = field;
    }
    @Override
    public Aggregator factorize(final ColumnSelectorFactory metricFactory)
    {
        return makeMergeAggregator(metricFactory);
    }

    @Override
    public byte[] getCacheKey() {
        return new CacheKeyBuilder(AggregatorUtil.GORILLA_MERGE_CACHE_TYPE_ID)
                .appendString(fieldName)
                .appendInt(DEFAULT_MAX_INTERMEDIATE_SIZE)
                .build();
    }

    private GorillaTsgMergeAggregator makeMergeAggregator(ColumnSelectorFactory columnFactory)
    {
        final BaseNullableColumnValueSelector selector = columnFactory.makeColumnValueSelector(fieldName);
        // null columns should be empty bloom filters by this point, so encountering a nil column in merge agg is unexpected
        if (selector instanceof NilColumnValueSelector) {
            throw new ISE("Unexpected NilColumnValueSelector");
        }
        ColumnCapabilities capabilities = columnFactory.getColumnCapabilities(fieldName);
        if (capabilities != null) {
            ValueType type = capabilities.getType();
            switch (type) {
                case COMPLEX:
                    return new GorillaTsgMergeAggregator(
                            columnFactory.makeColumnValueSelector(fieldName)
                    );
                default:
                    throw new IAE(
                            "Cannot create bloom filter %s for invalid column type [%s]",
                            false ? "aggregator" : "buffer aggregator",
                            type
                    );
            }
        }
        return null;
    }
}
