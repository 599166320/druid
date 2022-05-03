package org.apache.druid.query.aggregation;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.query.core.TSG;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ValueType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
@JsonTypeName(GorillaTscAggregatorFactory.TYPE_NAME)
public class GorillaTscAggregatorFactory extends AggregatorFactory{
    @Nonnull
    private final String fieldName;
    @Nonnull
    private final String name;
    protected final int maxIntermediateSize;
    private  static final int DEFAULT_NUM_ENTRIES = 3600/10;//最细5s一个点
    public  static final int  DEFAULT_MAX_INTERMEDIATE_SIZE = Long.BYTES*2+Double.BYTES+Integer.BYTES+1+Long.BYTES*DEFAULT_NUM_ENTRIES+Double.BYTES*DEFAULT_NUM_ENTRIES;
    public static final String TYPE_NAME = "gorilla";
    @JsonCreator
    public GorillaTscAggregatorFactory(
            @JsonProperty("name") final String name,
            @JsonProperty("fieldName") final String fieldName,
            @JsonProperty("maxIntermediateSize") @Nullable Integer maxIntermediateSize
    )
    {
        this.name = name;
        this.maxIntermediateSize = Objects.nonNull(maxIntermediateSize) && maxIntermediateSize>0?maxIntermediateSize:DEFAULT_MAX_INTERMEDIATE_SIZE;
        this.fieldName = fieldName;
    }

    /**
     * 堆内
     * @param metricFactory
     * @return
     */
    @Override
    public Aggregator factorize(ColumnSelectorFactory metricFactory) {
        return factorizeInternal(metricFactory,true);
    }

    /**
     * 堆外
     */
    @Override
    public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory) {
        return factorizeInternal(metricFactory,false);
    }

    private BaseGorillaTscAggregator factorizeInternal(ColumnSelectorFactory columnFactory, boolean onHeap) {
        ColumnCapabilities capabilities = columnFactory.getColumnCapabilities(fieldName);
        if (capabilities != null) {
            ValueType type = capabilities.getType();
            switch (type) {
                case COMPLEX:
                    return getBaseGorillaTscAggregator(columnFactory,onHeap);
                default:
                    throw new IAE(
                            "Cannot create bloom filter %s for invalid column type [%s]",
                            onHeap ? "aggregator" : "buffer aggregator",
                            type
                    );
            }
        }
        return getBaseGorillaTscAggregator(columnFactory,onHeap);
    }

    protected BaseGorillaTscAggregator getBaseGorillaTscAggregator(ColumnSelectorFactory columnFactory,boolean onHeap){
        return onHeap?new GorillaTscAggregator(
                columnFactory.makeColumnValueSelector(fieldName),
                maxIntermediateSize,
                onHeap
        ):new GorillaTsgBufferAggregator(
                columnFactory.makeColumnValueSelector(fieldName),
                maxIntermediateSize,
                onHeap
        );
    }

    @Override
    public byte[] getCacheKey() {
        return new CacheKeyBuilder(AggregatorUtil.GORILLA_CACHE_TYPE_ID)
                .appendString(fieldName)
                .appendInt(maxIntermediateSize)
                .build();
    }

    @Override
    public Comparator getComparator() {
        return null;
    }

    @Nullable
    @Override
    public Object combine(@Nullable Object lhs, @Nullable Object rhs) {
        if (rhs == null) {
            return lhs;
        }
        if (lhs == null) {
            return rhs;
        }
        return TSG.merge((byte[]) lhs,(byte[]) rhs);
    }

    @Override
    public AggregatorFactory getCombiningFactory() {
        return new GorillaTsgMergeAggregatorFactory(name,name,maxIntermediateSize);
    }

    @Override
    public List<AggregatorFactory> getRequiredColumns() {
        return ImmutableList.of(this);
    }

    @Override
    public Object deserialize(Object object) {
        if (object instanceof String) {
            return TSG.fromBytes(StringUtils.decodeBase64String((String) object));
        }  else {
            return object;
        }
    }

    @Nullable
    @Override
    public Object finalizeComputation(@Nullable Object object) {
        return object;
    }

    @JsonProperty
    @Override
    public String getName() {
        return name;
    }

    @Override
    public List<String> requiredFields() {
        return ImmutableList.of(fieldName);
    }

    @Override
    public ValueType getType() {
        return ValueType.COMPLEX;
    }

    @Override
    public ValueType getFinalizedType() {
        return ValueType.COMPLEX;
    }

    @Override
    public int getMaxIntermediateSize() {
        return maxIntermediateSize;
    }

    @JsonProperty
    public String getFieldName()
    {
        return fieldName;
    }


    @JsonProperty
    public int maxIntermediateSize()
    {
        return maxIntermediateSize;
    }
    @Override
    public boolean equals(final Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || !getClass().equals(o.getClass())) {
            return false;
        }
        final GorillaTscAggregatorFactory that = (GorillaTscAggregatorFactory) o;

        return Objects.equals(name, that.name) &&
                Objects.equals(fieldName, that.fieldName) &&
                maxIntermediateSize == that.maxIntermediateSize;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, fieldName, maxIntermediateSize);
    }

    @Override
    public String getComplexTypeName()
    {
        return TYPE_NAME;
    }
}
