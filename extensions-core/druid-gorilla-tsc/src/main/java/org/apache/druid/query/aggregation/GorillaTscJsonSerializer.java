package org.apache.druid.query.aggregation;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.druid.tsg.TSG;


import java.io.IOException;

public class GorillaTscJsonSerializer extends JsonSerializer<TSG>
{
    @Override
    public void serialize(
            TSG tsg,
            JsonGenerator jsonGenerator,
            SerializerProvider serializerProvider
    ) throws IOException
    {
        jsonGenerator.writeBinary(tsg.toBytes());
    }

}
