package org.apache.druid.query.aggregation.quantile;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.segment.ColumnValueSelector;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.IdentityHashMap;
public class ValueAppendBufferAggregator implements BufferAggregator
{
    private static final Logger log = new Logger(ValueAppendBufferAggregator.class);
    private final IdentityHashMap<ByteBuffer, Int2ObjectMap<ValueCollection>> cache = new IdentityHashMap();
    private final ColumnValueSelector selector;
    public ValueAppendBufferAggregator(ColumnValueSelector selector) {
        this.selector = selector;
    }

    @Override
    public void init(ByteBuffer buf, int position) {
        ValueCollection values = new ValueCollection();
        addToCache(buf,position,values);
    }

    @Override
    public void aggregate(ByteBuffer buf, int position) {
        Int2ObjectMap<ValueCollection> int2ObjectMap = cache.get(buf);
        ValueCollection values = null;
        if(int2ObjectMap != null){
            values = int2ObjectMap.get(position);
        }
        Object obj = selector.getObject();
        if( obj != null){
            if(obj instanceof ValueCollection){
                values.addAll((ValueCollection) obj);
            }else if(obj instanceof Integer){
                values.add((Integer) obj,1);
            } else if(obj instanceof byte[]){
                values.addAll(ValueCollection.deserialize((byte[]) obj));
            }else if(obj instanceof String){
                values.addAll(ValueCollection.deserialize(StringUtils.decodeBase64(StringUtils.toUtf8((String) obj))));
            }
        }
    }

    @Nullable
    @Override
    public Object get(ByteBuffer buf, int position) {
        ValueCollection tmp = cache.get(buf).get(position);
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
        ValueCollection values = cache.get(oldBuffer).get(oldPosition);
        addToCache(newBuffer, newPosition, values);
        final Int2ObjectMap<ValueCollection> map = cache.get(oldBuffer);
        map.remove(oldPosition);
        if (map.isEmpty()) {
            cache.remove(oldBuffer);
        }
    }

    private void addToCache(final ByteBuffer buffer, final int position, final ValueCollection values)
    {
        Int2ObjectMap<ValueCollection> map = cache.computeIfAbsent(buffer, b -> new Int2ObjectOpenHashMap<>());
        map.put(position, values);
    }
}
