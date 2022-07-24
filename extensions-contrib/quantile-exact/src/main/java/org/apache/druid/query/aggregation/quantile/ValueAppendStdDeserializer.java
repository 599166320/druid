package org.apache.druid.query.aggregation.quantile;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

public class ValueAppendStdDeserializer extends StdDeserializer<ValueCollection>
{
    public ValueAppendStdDeserializer()
    {
        super(ArrayList.class);
    }

    @Override
    public ValueCollection deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
            throws IOException
    {
        return ValueCollection.deserialize(jsonParser.getBinaryValue());
    }
}
