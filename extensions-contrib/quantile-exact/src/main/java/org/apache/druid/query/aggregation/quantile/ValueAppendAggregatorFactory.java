package org.apache.druid.query.aggregation.quantile;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Longs;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.AggregatorUtil;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.column.ValueType;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.*;
@JsonTypeName(ValueAppendAggregatorFactory.TYPE_NAME)
public class ValueAppendAggregatorFactory extends AggregatorFactory
{
    public static final int DEFAULT_MAX_INTERMEDIATE_SIZE = 1;
    public static final String TYPE_NAME = "valueAppend";
    private final String name;
    private final String fieldName;
    private final String fun;
    private final int maxIntermediateSize;
    @JsonCreator
    public ValueAppendAggregatorFactory(
            @JsonProperty("name") String name,
            @JsonProperty("fieldName") String fieldName,
            @JsonProperty("maxIntermediateSize") int maxIntermediateSize,
            @JsonProperty("fun") String fun
    )
    {
        Preconditions.checkNotNull(name);
        Preconditions.checkNotNull(fieldName);
        this.name = name;
        this.fieldName = fieldName;
        this.fun = fun;
        this.maxIntermediateSize = maxIntermediateSize;
    }

    @Override
    public byte[] getCacheKey() {
        byte[] fieldNameBytes = StringUtils.toUtf8(fieldName);
        return ByteBuffer.allocate(2 + fieldNameBytes.length)
                .put(AggregatorUtil.DISTINCT_COUNT_CACHE_KEY)
                .put(fieldNameBytes)
                .put(AggregatorUtil.STRING_SEPARATOR)
                .array();
    }

    @Override
    public Aggregator factorize(ColumnSelectorFactory columnFactory) {
        return new ValueAppendAggregator(columnFactory.makeColumnValueSelector(fieldName));
    }

    @Override
    public BufferAggregator factorizeBuffered(ColumnSelectorFactory columnFactory) {
        return new ValueAppendBufferAggregator(columnFactory.makeColumnValueSelector(fieldName));
    }

    @Override
    public Comparator getComparator() {
        return new Comparator()
        {
            @Override
            public int compare(Object o, Object o1)
            {
                return Longs.compare(((Number) o).longValue(), ((Number) o1).longValue());
            }
        };
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
        ((ArrayList<Double>) lhs).addAll((ArrayList<Double>)rhs);
        return lhs;
    }

    @Override
    public AggregatorFactory getCombiningFactory() {
        return new ValueAppendAggregatorFactory(name,name,maxIntermediateSize,fun);
    }

    @Override
    public List<AggregatorFactory> getRequiredColumns() {
        return Collections.singletonList(new ValueAppendAggregatorFactory(
                fieldName,
                fieldName,
                maxIntermediateSize,
                fun
        ));
    }

    @Override
    public Object deserialize(Object object) {
        final ByteBuffer buffer;
        if (object instanceof byte[]) {
            buffer = ByteBuffer.wrap((byte[]) object);
        } else if (object instanceof ByteBuffer) {
            // Be conservative, don't assume we own this buffer.
            buffer = ((ByteBuffer) object).duplicate();
        } else if (object instanceof String) {
            buffer = ByteBuffer.wrap(StringUtils.decodeBase64(StringUtils.toUtf8((String) object)));
        } else {
            return object;
        }
        ArrayList<Double> list = new ArrayList<>();
        while(buffer.hasRemaining()){
            list.add(buffer.getDouble());
        }
        return list;
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
        return Collections.singletonList(fieldName);
    }

    @JsonProperty
    public String getFieldName() {
        return fieldName;
    }

    @Override
    public ValueType getType() {
        return ValueType.COMPLEX;
    }

    @Override
    public ValueType getFinalizedType() {
        return ValueType.COMPLEX;
    }

    @JsonProperty
    @Override
    public int getMaxIntermediateSize() {
        return maxIntermediateSize;
    }

    @JsonProperty
    public String getFun() {
        return fun;
    }

    @Override
    public String getComplexTypeName() {
        return TYPE_NAME;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ValueAppendAggregatorFactory that = (ValueAppendAggregatorFactory) o;
        return maxIntermediateSize == that.maxIntermediateSize && Objects.equals(name, that.name) && Objects.equals(fieldName, that.fieldName) && Objects.equals(fun, that.fun);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, fieldName, fun, maxIntermediateSize);
    }
}
