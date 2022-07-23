package org.apache.druid.query.aggregation.quantile;

import org.apache.druid.segment.data.ObjectStrategy;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.ArrayList;

public class ValueAppendHolderObjectStrategy implements ObjectStrategy<ArrayList> {

    @Override
    public Class<? extends ArrayList> getClazz() {
        return ArrayList.class;
    }

    @Nullable
    @Override
    public ArrayList<Double> fromByteBuffer(ByteBuffer buffer, int numBytes) {
        if(numBytes == 0){
            return null;
        }
        ByteBuffer readOnlyBuffer = buffer.asReadOnlyBuffer();
        readOnlyBuffer.limit(buffer.position() + numBytes);

        ArrayList<Double> values = new ArrayList<>();
        while(readOnlyBuffer.hasRemaining()){
            values.add(readOnlyBuffer.getDouble());
        }
        return values;
    }


    @Nullable
    @Override
    public byte[] toBytes(@Nullable ArrayList values) {
        ByteBuffer buffer = ByteBuffer.allocate(Double.BYTES * values.size());
        for(Object v:values){
            buffer.putDouble((Double) v);
        }
        return buffer.array();
    }

    @Override
    public int compare(ArrayList o1, ArrayList o2) {
        return 0;
    }
}