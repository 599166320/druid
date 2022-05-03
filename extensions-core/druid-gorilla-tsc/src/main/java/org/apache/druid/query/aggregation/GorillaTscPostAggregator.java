package org.apache.druid.query.aggregation;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.aggregation.post.PostAggregatorIds;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.query.core.DataPoint;
import org.apache.druid.query.core.TSG;
import org.apache.druid.segment.column.ValueType;
import javax.annotation.Nullable;
import java.util.*;
public class GorillaTscPostAggregator implements PostAggregator{

    private static final Logger log = new Logger(GorillaTscPostAggregator.class);
    private final String name;
    private final PostAggregator fieldName;
    private final String fun;
    private final long start;
    private final long end;
    private final int interval;
    private final int range;
    public static final String TYPE_NAME = "timeSeriesFromTSG";

    @JsonCreator
    public GorillaTscPostAggregator(
            @JsonProperty("name") final String name,
            @JsonProperty("fieldName") final PostAggregator fieldName,
            @JsonProperty("fun") final String fun,
            @JsonProperty("start") final long start,
            @JsonProperty("end") final long end,
            @JsonProperty("interval") final int interval,
            @JsonProperty("range") final int range
    )
    {
        this.name = Preconditions.checkNotNull(name, "name is null");
        this.fieldName = Preconditions.checkNotNull(fieldName, "field is null");
        this.fun = fun;
        this.start = start;
        this.end = end;
        this.interval = interval;
        this.range = range;
    }

    @Override
    public byte[] getCacheKey() {
        final CacheKeyBuilder builder = new CacheKeyBuilder(
                PostAggregatorIds.GORILLA_TSC_TO_TIME_SERIES_CACHE_TYPE_ID).appendCacheable(fieldName);
        builder.appendString(fun);
        builder.appendLong(start);
        builder.appendLong(end);
        builder.appendLong(interval);
        builder.appendLong(range);
        return builder.build();
    }

    @Override
    public Set<String> getDependentFields() {
        return  fieldName.getDependentFields();
    }

    @Override
    public Comparator<TSG> getComparator() {
        return TSG::compare;
    }

    @Nullable
    @Override
    public Object compute(Map<String, Object> combinedAggregators) {

        Object tmp = fieldName.compute(combinedAggregators);
        TSG tsg = null;
        if(tmp instanceof TSG){
            tsg = (TSG) tmp;

        }else if(tmp instanceof byte[]){
            tsg = TSG.fromBytes((byte[])tmp);
        }
        if( tsg != null){
            if(start>0 && end >0){
                Iterator<DataPoint> tsgIterator = tsg.toIterator();
                TreeMap<Long,Double> treeMap = new TreeMap<>();
                while (tsgIterator.hasNext()){
                    DataPoint p = tsgIterator.next();
                    if(p.getTime()>= start && p.getTime() <= end){
                        treeMap.put(p.getTime(),p.getValue());
                    }
                }
                if(treeMap.size() >0){
                    TSG result = new TSG(treeMap.firstKey());
                    for(Map.Entry<Long,Double> e:treeMap.entrySet()){
                        result.put(e.getKey(),e.getValue());
                    }
                    return result.toBytes();
                }
            }
        }
        return tmp;
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

    @JsonProperty
    public long getStart() {
        return start;
    }

    @JsonProperty
    public long getEnd() {
        return end;
    }

    @JsonProperty
    public int getInterval() {
        return interval;
    }

    @JsonProperty
    public int getRange() {
        return range;
    }

    @Nullable
    @Override
    public ValueType getType() {
        return ValueType.COMPLEX;
    }

    @Override
    public PostAggregator decorate(Map<String, AggregatorFactory> aggregators) {
        return this;
    }


    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "{" +
                "name='" + name + '\'' +
                ", fieldName=" + fieldName +
                ", fun=" + fun +
                ", start=" + start +
                ", end=" + end +
                ", interval=" + interval +
                ", range=" + range +
                "}";
    }
    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GorillaTscPostAggregator that = (GorillaTscPostAggregator) o;
        return Long.compare(that.start, start) == 0 &&
                Long.compare(that.end, end) == 0 &&
                Long.compare(that.interval, interval) == 0 &&
                Long.compare(that.range, range) == 0 &&
                Objects.equals(fun, that.fun) &&
                Objects.equals(name, that.name) &&
                Objects.equals(fieldName, that.fieldName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, fieldName,fun, start,end,interval,range);
    }
}
