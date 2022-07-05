package org.apache.druid.query.aggregation;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.tsg.DataPoint;
import org.apache.druid.tsg.TSG;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.IdentityHashMap;
public class GorillaTsgBufferAggregator extends  BaseGorillaTscAggregator<ColumnValueSelector>{

    private final IdentityHashMap<ByteBuffer, Int2ObjectMap<TSG>> tsgCache = new IdentityHashMap();

    public GorillaTsgBufferAggregator(ColumnValueSelector columnValueSelector, int maxNumEntries, boolean onHeap) {
        super(columnValueSelector,maxNumEntries,onHeap);
    }

    @Override
    public void aggregate(ByteBuffer buf, int position) {
        Int2ObjectMap<TSG> int2ObjectMap = tsgCache.get(buf);
        TSG tsg = null;
        if(int2ObjectMap != null){
            tsg = int2ObjectMap.get(position);
        }
        Object obj = selector.getObject();
        if(tsg == null && obj != null){
            if(obj instanceof Object[][]){
                Object[][] timeAndValues = (Object[][]) obj;
                if(timeAndValues.length > 0 && timeAndValues[0].length> 0){
                    long ts = (long) (timeAndValues)[0][0];
                    tsg = new TSG(ts-ts%(3600));
                    addToCache(buf,position,tsg);
                }else{
                    return;
                }
            }else if(obj instanceof  TSG){
                tsg = (TSG) obj;
                addToCache(buf,position,tsg);
                return;
            }else if(obj instanceof byte[]){
                tsg = TSG.fromBytes((byte[])obj);
                addToCache(buf,position,tsg);
                return;
            }else if(obj instanceof String){
                tsg = TSG.fromBytes(StringUtils.decodeBase64String((String)obj));
                addToCache(buf,position,tsg);
                return;
            }
        }
        if (obj == null) {
            return;
        }else if(obj instanceof TSG){
            TSG other = (TSG) obj;
            tsg = TSG.merge(tsg,other);
        }else if(obj instanceof byte[]){
            TSG other = TSG.fromBytes((byte[])obj);
            tsg = TSG.merge(tsg,other);
        }else if(obj instanceof String){
            TSG other =  TSG.fromBytes(StringUtils.decodeBase64String((String)obj));
            tsg = TSG.merge(tsg,other);
        }else if(obj instanceof Object[][]){
            //聚合的时候使用
            tsg = addNewPoint((Object[][]) obj,tsg);
        }else if(obj instanceof DataPoint){
            //聚合的时候使用
            tsg.put((DataPoint)obj);
        }
        addToCache(buf,position,tsg);
    }

    @Nullable
    @Override
    public Object get(ByteBuffer buf, int position) {
        Object tmp = tsgCache.get(buf).get(position);;
        return tmp;
    }

    @Override
    public Object get() {
        Object tmp = selector.getObject();
        return tmp;
    }

    @Override
    public void close() {
        tsgCache.clear();
    }

    @Override
    public void relocate(int oldPosition, int newPosition, ByteBuffer oldBuffer, ByteBuffer newBuffer) {
        TSG tsg = tsgCache.get(oldBuffer).get(oldPosition);
        addToCache(newBuffer, newPosition, tsg);
        final Int2ObjectMap<TSG> map = tsgCache.get(oldBuffer);
        map.remove(oldPosition);
        if (map.isEmpty()) {
            tsgCache.remove(oldBuffer);
        }
    }

    private void addToCache(final ByteBuffer buffer, final int position, final TSG tsg)
    {
        Int2ObjectMap<TSG> map = tsgCache.computeIfAbsent(buffer, b -> new Int2ObjectOpenHashMap<>());
        map.put(position, tsg);
    }
}
