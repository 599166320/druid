package org.apache.druid.query.aggregation.quantile;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.segment.ColumnValueSelector;
import javax.annotation.Nullable;
public class ValueAppendAggregator implements Aggregator
{
    private final ColumnValueSelector selector;
    protected ValueCollection values;
    public ValueAppendAggregator(ColumnValueSelector selector){
        this.selector = selector;
    }

    @Override
    public void aggregate() {
        Object obj = selector.getObject();
        if(values == null && obj != null){
            if(obj instanceof Integer){
                values = new ValueCollection();
                values.add(bucket((Integer) obj),1);
            }else if(obj instanceof ValueCollection){
                values = (ValueCollection) obj;
            }else if(obj instanceof byte[]){
                values = ValueCollection.deserialize((byte[]) obj);
            }else if(obj instanceof String){
                values = ValueCollection.deserialize(StringUtils.decodeBase64(StringUtils.toUtf8((String) obj)));
            }
        }else if(obj != null){
            if(obj instanceof Integer){
                values.add(bucket((Integer) obj),1);
            }else if(obj instanceof ValueCollection){
                values.addAll((ValueCollection) obj);
            }else if(obj instanceof byte[]){
                values.addAll(ValueCollection.deserialize((byte[]) obj));
            }else if(obj instanceof String){
                values.addAll(ValueCollection.deserialize(StringUtils.decodeBase64(StringUtils.toUtf8((String) obj))));
            }
        }
    }

    private int bucket(int v){
        if(v > 100000){
            if(v > 600000){
                return 655000;
            }else if(v > 500000){
                return 555000;
            }else if(v > 400000){
                return 455000;
            }else if(v > 300000){
                return 355000;
            }else if(v > 200000){
                return 200000;
            }else {
                return 155000;
            }
        }else if( v < 100){
            if(v >90){
                return 95;
            }else if(v > 80){
                return 85;
            }
            return 0;
        }
        return v;
    }


    @Nullable
    @Override
    public Object get() {
        return values;
    }

    @Override
    public float getFloat() {
        return 0;
    }

    @Override
    public long getLong() {
        return 0;
    }

    @Override
    public void close() {
        values = null;
    }
}
