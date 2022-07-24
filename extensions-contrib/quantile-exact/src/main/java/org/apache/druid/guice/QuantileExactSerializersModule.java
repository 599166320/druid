package org.apache.druid.guice;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.annotations.VisibleForTesting;
import org.apache.druid.query.aggregation.quantile.*;
import org.apache.druid.query.aggregation.sql.QuantileExactPostAggregator;
import org.apache.druid.segment.serde.ComplexMetrics;
import java.util.ArrayList;
public class QuantileExactSerializersModule extends SimpleModule
{
    public QuantileExactSerializersModule()
    {
        registerSubtypes(
                new NamedType(ValueAppendAggregatorFactory.class, ValueAppendAggregatorFactory.TYPE_NAME),
                new NamedType(QuantileExactPostAggregator.class, QuantileExactPostAggregator.TYPE_NAME)
        );
        addSerializer(ValueCollection.class, new ValueAppendJsonSerializer());
        addDeserializer(ValueCollection.class, new ValueAppendStdDeserializer());
        registerSerde();
    }

    @VisibleForTesting
    public static void registerSerde()
    {
        ComplexMetrics.registerSerde(ValueAppendAggregatorFactory.TYPE_NAME, new ValueAppendComplexMetricSerde());
        ComplexMetrics.registerSerde(QuantileExactPostAggregator.TYPE_NAME, new ValueAppendComplexMetricSerde());
    }
}
