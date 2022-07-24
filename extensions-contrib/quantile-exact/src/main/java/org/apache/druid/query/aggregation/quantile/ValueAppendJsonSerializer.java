package org.apache.druid.query.aggregation.quantile;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
public class ValueAppendJsonSerializer extends JsonSerializer<ValueCollection>
{
    @Override
    public void serialize(
            ValueCollection values,
            JsonGenerator jsonGenerator,
            SerializerProvider serializerProvider
    ) throws IOException
    {
        jsonGenerator.writeBinary(values.serialize());
    }

}
