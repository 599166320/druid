package org.apache.druid.query.aggregation.quantile;

import org.apache.druid.segment.data.ObjectStrategy;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.ArrayList;

public class ValueAppendHolderObjectStrategy implements ObjectStrategy<ValueCollection> {

    @Override
    public Class<? extends ValueCollection> getClazz() {
        return ValueCollection.class;
    }

    @Nullable
    @Override
    public ValueCollection fromByteBuffer(ByteBuffer buffer, int numBytes) {
        if(numBytes == 0){
            return null;
        }
        ByteBuffer readOnlyBuffer = buffer.asReadOnlyBuffer();
        readOnlyBuffer.limit(buffer.position() + numBytes);
        ByteBuffer newBuff = ByteBuffer.allocate(numBytes);
        while(readOnlyBuffer.hasRemaining()){
            newBuff.put(readOnlyBuffer.get());
        }
        newBuff.flip();
        return ValueCollection.deserialize(newBuff);
    }


    @Nullable
    @Override
    public byte[] toBytes(@Nullable ValueCollection values) {
        return values.serialize();
    }

    @Override
    public int compare(ValueCollection o1, ValueCollection o2) {
        return 0;
    }
}