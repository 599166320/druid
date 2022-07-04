package org.apache.druid.query.aggregation;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.druid.query.cache.CacheKeyBuilder;
import javax.annotation.Nullable;
import java.util.Objects;
@JsonTypeName(GorillaTscQueryAggregatorFactory.TYPE_NAME)
public class GorillaTscQueryAggregatorFactory extends GorillaTscAggregatorFactory{
    public static final String TYPE_NAME = "gorilla_tsc_merge";
    private final String fun;
    private final long start;
    private final long end;
    @JsonCreator
    public GorillaTscQueryAggregatorFactory(
            @JsonProperty("name") final String name,
            @JsonProperty("fieldName") final String fieldName,
            @JsonProperty("maxIntermediateSize") @Nullable Integer maxIntermediateSize,
            @JsonProperty("fun") final String fun,
            @JsonProperty("start") final long start,
            @JsonProperty("end") final long end
    )
    {
       super(name,fieldName,maxIntermediateSize);
        this.fun = fun;
        this.start = start;
        this.end = end;
    }
    @JsonProperty
    public String getFun() {
        return fun;
    }
    @JsonProperty
    public long getStart() {
        return start;
    }
    @JsonProperty
    public long getEnd() {
        return end;
    }

    @Override
    public byte[] getCacheKey() {
        return new CacheKeyBuilder(AggregatorUtil.GORILLA_CACHE_TYPE_ID)
                .appendString(fieldName)
                .appendInt(maxIntermediateSize)
                .appendString(fun)
                .appendLong(start)
                .appendLong(end)
                .build();
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
        final GorillaTscQueryAggregatorFactory that = (GorillaTscQueryAggregatorFactory) o;

        return Objects.equals(name, that.name) &&
                Objects.equals(fieldName, that.fieldName) &&
                Objects.equals(fun, that.fun) &&
                start == that.start &&
                end == that.end &&
                maxIntermediateSize == that.maxIntermediateSize;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, fieldName, maxIntermediateSize,fun,start,end);
    }

    @Override
    public String getComplexTypeName()
    {
        return TYPE_NAME;
    }

}
