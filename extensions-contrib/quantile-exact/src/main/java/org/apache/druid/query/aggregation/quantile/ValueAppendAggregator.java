package org.apache.druid.query.aggregation.quantile;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.ArrayList;

public class ValueAppendAggregator implements Aggregator
{
    private final ColumnValueSelector selector;
    protected ArrayList<Double> values;
    public ValueAppendAggregator(ColumnValueSelector selector){
        this.selector = selector;
    }

    @Override
    public void aggregate() {
        Object obj = selector.getObject();
        if(values == null && obj != null){
            values = new ArrayList<>();
            if(obj instanceof Double){
                values.add((Double) obj);
            }else if(obj instanceof ArrayList){
                values = (ArrayList<Double>) obj;
            }else if(obj instanceof byte[]){
                ByteBuffer buffer = ByteBuffer.wrap((byte[]) obj);
                while(buffer.hasRemaining()){
                    values.add(buffer.getDouble());
                }
            }else if(obj instanceof String){
                ByteBuffer buffer =  ByteBuffer.wrap(StringUtils.decodeBase64(StringUtils.toUtf8((String) obj)));
                while(buffer.hasRemaining()){
                    values.add(buffer.getDouble());
                }
            }
        }else if(obj != null){
            if(obj instanceof Double){
                values.add((Double) obj);
            }else if(obj instanceof ArrayList){
                values.addAll((ArrayList<Double>) obj);
            }else if(obj instanceof byte[]){
                ByteBuffer buffer = ByteBuffer.wrap((byte[]) obj);
                while(buffer.hasRemaining()){
                    values.add(buffer.getDouble());
                }
            }else if(obj instanceof String){
                ByteBuffer buffer =  ByteBuffer.wrap(StringUtils.decodeBase64(StringUtils.toUtf8((String) obj)));
                while(buffer.hasRemaining()){
                    values.add(buffer.getDouble());
                }
            }
        }
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
