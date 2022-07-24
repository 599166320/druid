package org.apache.druid.query.aggregation.sql;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.api.client.util.Preconditions;
import com.google.common.primitives.Longs;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.aggregation.post.PostAggregatorIds;
import org.apache.druid.query.aggregation.quantile.ValueCollection;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.segment.column.ValueType;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.*;
public class QuantileExactPostAggregator implements PostAggregator {
    private static final Logger log = new Logger(QuantileExactPostAggregator.class);
    private final String name;
    private final PostAggregator fieldName;
    private final String fun;
    public static final String TYPE_NAME = "valuesAppend";
    @JsonCreator
    public QuantileExactPostAggregator(
            @JsonProperty("name") final String name,
            @JsonProperty("fieldName") final PostAggregator fieldName,
            @JsonProperty("fun") final String fun
    )
    {
        this.name = Preconditions.checkNotNull(name, "name is null");
        this.fieldName = Preconditions.checkNotNull(fieldName, "field is null");
        this.fun = fun;
    }
    @Override
    public byte[] getCacheKey() {
        final CacheKeyBuilder builder = new CacheKeyBuilder(
                PostAggregatorIds.QUANTILE_EXACT_CACHE_TYPE_ID).appendCacheable(fieldName);
        builder.appendString(fun);
        return builder.build();
    }

    @Override
    public Set<String> getDependentFields() {
        return  fieldName.getDependentFields();
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
    public Object compute(Map<String, Object> combinedAggregators) {
        Object tmp = fieldName.compute(combinedAggregators);
        ValueCollection values = null;
        if(tmp instanceof ValueCollection){
            values = (ValueCollection) tmp;
        }else if(tmp instanceof byte[]){
            values = ValueCollection.deserialize((byte[]) tmp);
        }
        String [] funs = fun.split(",");
        double [] results = new double[funs.length];
        int i = 0;
        for(String f : funs){
            if("max".equals(f)){
                results[i] = values.max();
            }else if("min".equals(f)){
                results[i] = values.min();
            }else if("mean".equals(f)){
                results[i] = values.mean();
            }else if(f.startsWith("p")){
                double p = Double.valueOf(f.replace("p",""));
                int pidx = (int) (values.count()*p);
                results[i] = values.get(pidx);
            }
            i++;
        }
        return results;
    }

    @Nullable
    @Override
    @JsonProperty
    public String getName() {
        return name;
    }
    @JsonProperty
    public PostAggregator getFieldName() {
        return fieldName;
    }
    @JsonProperty
    public String getFun() {
        return fun;
    }

    @Nullable
    @Override
    public ValueType getType() {
        return ValueType.DOUBLE_ARRAY;
    }

    @Override
    public PostAggregator decorate(Map<String, AggregatorFactory> aggregators) {
        return this;
    }
}
