package org.apache.druid.query.aggregation;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import org.apache.druid.query.core.TSG;

import java.io.IOException;

public class TSGTscJsonDeserializer extends StdDeserializer<TSG> {
    public TSGTscJsonDeserializer()
    {
        super(TSG.class);
    }

    @Override
    public TSG deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
            throws IOException
    {
        return TSG.fromBytes(jsonParser.getBinaryValue());
    }
}
