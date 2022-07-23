package org.apache.druid.query.aggregation.quantile;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.segment.ColumnValueSelector;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.IdentityHashMap;
public class ValueAppendBufferAggregator implements BufferAggregator
{
    private final IdentityHashMap<ByteBuffer, Int2ObjectMap<ArrayList<Double>>> cache = new IdentityHashMap();
    private final ColumnValueSelector selector;
    public ValueAppendBufferAggregator(ColumnValueSelector selector) {
        this.selector = selector;
    }

    @Override
    public void init(ByteBuffer buf, int position) {
    }

    @Override
    public void aggregate(ByteBuffer buf, int position) {
        Int2ObjectMap<ArrayList<Double>> int2ObjectMap = cache.get(buf);
        ArrayList<Double> values = null;
        if(int2ObjectMap != null){
            values = int2ObjectMap.get(position);
        }
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
        addToCache(buf,position,values);
    }

    @Nullable
    @Override
    public Object get(ByteBuffer buf, int position) {
        Object tmp = cache.get(buf).get(position);
        return tmp;
    }

    @Override
    public float getFloat(ByteBuffer buf, int position) {
        return 0;
    }

    @Override
    public long getLong(ByteBuffer buf, int position) {
        return 0;
    }

    @Override
    public void close() {
        cache.clear();
    }

    @Override
    public void relocate(int oldPosition, int newPosition, ByteBuffer oldBuffer, ByteBuffer newBuffer) {
        ArrayList<Double> values = cache.get(oldBuffer).get(oldPosition);
        addToCache(newBuffer, newPosition, values);
        final Int2ObjectMap<ArrayList<Double>> map = cache.get(oldBuffer);
        map.remove(oldPosition);
        if (map.isEmpty()) {
            cache.remove(oldBuffer);
        }
    }

    private void addToCache(final ByteBuffer buffer, final int position, final ArrayList<Double> values)
    {
        Int2ObjectMap<ArrayList<Double>> map = cache.computeIfAbsent(buffer, b -> new Int2ObjectOpenHashMap<>());
        map.put(position, values);
    }
}
