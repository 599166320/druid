package org.apache.druid.query.aggregation.quantile;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.collections.StupidPool;
import java.nio.ByteBuffer;
/**
 * buff溢出，刷到map中
 */
public class ValueCollectionWithBytebuffer extends ValueCollection{
    private static final int BUFF_LEN = 256;
    private static final StupidPool BUFF_POOL = new StupidPool<>("quantile", () -> ByteBuffer.allocate(BUFF_LEN));
    private ResourceHolder<ByteBuffer> resourceHolder;
    private ByteBuffer buffer;
    public ValueCollectionWithBytebuffer(){
        resourceHolder = BUFF_POOL.take();
        buffer = resourceHolder.get();
    }

    public static  ValueCollectionWithBytebuffer deserialize(byte[] datas){
        ValueCollectionWithBytebuffer values = new ValueCollectionWithBytebuffer();
        ByteBuffer b = ByteBuffer.wrap(datas);
        b.flip();
        while (b.hasRemaining()){
            values.add(b.getInt(),b.getInt());
        }
        return values;
    }


    @Override
    public void add(Integer k, Integer c) {
        if(buffer.hasRemaining()){
            buffer.putInt(k);
            buffer.putInt(c);
        }else {
            super.add(k, c);
            buffer.flip();
            while (buffer.hasRemaining()){
                super.add(buffer.getInt(),buffer.getInt());
            }
            buffer.clear();
        }
    }

    public void addAll(ValueCollectionWithBytebuffer other) {
        super.addAll(other);
        ValueCollectionWithBytebuffer o = (ValueCollectionWithBytebuffer) other;
        while (o.buffer.hasRemaining()){
            super.add(o.buffer.getInt(),o.buffer.getInt());
        }
        o.close();
    }

    @Override
    public byte[] serialize() {
        buffer.flip();
        while (buffer.hasRemaining()){
            super.add(buffer.getInt(),buffer.getInt());
        }
        return super.serialize();
    }

    public void close(){
        resourceHolder.get().clear();
        resourceHolder.close();
    }

}
