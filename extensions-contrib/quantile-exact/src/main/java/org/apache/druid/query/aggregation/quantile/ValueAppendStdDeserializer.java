package org.apache.druid.query.aggregation.quantile;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

public class ValueAppendStdDeserializer extends StdDeserializer<ArrayList>
{
    public ValueAppendStdDeserializer()
    {
        super(ArrayList.class);
    }

    @Override
    public ArrayList deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
            throws IOException
    {
        ByteBuffer buffer = ByteBuffer.wrap(jsonParser.getBinaryValue());
        ArrayList<Double> values = new ArrayList<>();
        while(buffer.hasRemaining()){
            values.add(buffer.getDouble());
        }
        return values;
    }
}
