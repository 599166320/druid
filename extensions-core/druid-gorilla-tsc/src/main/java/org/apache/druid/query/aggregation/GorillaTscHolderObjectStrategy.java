package org.apache.druid.query.aggregation;
import org.apache.druid.query.core.OutBitSet;
import org.apache.druid.query.core.TSG;
import org.apache.druid.segment.data.ObjectStrategy;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public class GorillaTscHolderObjectStrategy  implements ObjectStrategy<TSG> {

    @Override
    public Class<? extends TSG> getClazz() {
        return TSG.class;
    }

    @Nullable
    @Override
    public TSG fromByteBuffer(ByteBuffer buffer, int numBytes) {
        if(numBytes == 0){
            return null;
        }
        ByteBuffer readOnlyBuffer = buffer.asReadOnlyBuffer();
        readOnlyBuffer.limit(buffer.position() + numBytes);
        return TSG.fromBytes(readOnlyBuffer);
    }

    @Nullable
    @Override
    public byte[] toBytes(@Nullable TSG val) {
        return val.toBytes();
    }

    @Override
    public int compare(TSG o1, TSG o2) {
        return 0;
    }
}
