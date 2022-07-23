package org.apache.druid.query.aggregation.quantile;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
public class ValueAppendJsonSerializer extends JsonSerializer<ArrayList>
{
    @Override
    public void serialize(
            ArrayList values,
            JsonGenerator jsonGenerator,
            SerializerProvider serializerProvider
    ) throws IOException
    {
        ByteBuffer buffer = ByteBuffer.allocate(Double.BYTES*values.size());
        for(Object v:values){
            buffer.putDouble((Double) v);
        }
        jsonGenerator.writeBinary(buffer.array());
    }

}
