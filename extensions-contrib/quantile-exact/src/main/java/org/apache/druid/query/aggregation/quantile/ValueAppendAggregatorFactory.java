package org.apache.druid.query.aggregation.quantile;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Longs;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.aggregation.*;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
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
                .put(AggregatorUtil.VALUE_APPEND_CACHE_TYPE_ID)
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
        ((ValueCollection) lhs).addAll((ValueCollection)rhs);
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
        if (object instanceof byte[]) {
            return ValueCollection.deserialize((byte[]) object);
        } else if (object instanceof ByteBuffer) {
            // Be conservative, don't assume we own this buffer.
            return ValueCollection.deserialize(((ByteBuffer) object).duplicate().array());
        } else if (object instanceof String) {
           return ValueCollection.deserialize(StringUtils.decodeBase64(StringUtils.toUtf8((String) object)));
        } else {
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


    @Override
    public AggregateCombiner makeAggregateCombiner() {
        return new ObjectAggregateCombiner() {
            private ValueCollection combined;
            @Override
            public void reset(ColumnValueSelector selector) {
                combined = null;
                fold(selector);
            }

            @Override
            public void fold(ColumnValueSelector selector) {
                ValueCollection other = (ValueCollection) selector.getObject();
                if (other == null) {
                    return;
                }
                if (combined == null) {
                    combined = other;
                }else {
                    combined.addAll(other);
                }
            }

            @Nullable
            @Override
            public Object getObject() {
                return combined;
            }

            @Override
            public Class classOfObject() {
                return ValueCollection.class;
            }
        };
    }

}
