package org.apache.druid.query.aggregation.quantile;

import org.apache.druid.data.input.InputRow;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.GenericColumnSerializer;
import org.apache.druid.segment.column.ColumnBuilder;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.ObjectStrategy;
import org.apache.druid.segment.serde.ComplexColumnPartSupplier;
import org.apache.druid.segment.serde.ComplexMetricExtractor;
import org.apache.druid.segment.serde.ComplexMetricSerde;
import org.apache.druid.segment.serde.LargeColumnSupportedComplexColumnSerializer;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.List;

public class ValueAppendComplexMetricSerde extends ComplexMetricSerde
{
    private static final ValueAppendHolderObjectStrategy STRATEGY = new ValueAppendHolderObjectStrategy();
    @Override
    public String getTypeName()
    {
        return ValueAppendAggregatorFactory.TYPE_NAME;
    }

    @Override
    public ComplexMetricExtractor getExtractor()
    {
        return new ComplexMetricExtractor(){
            @Override
            public Class extractedClass() {
                return ValueCollection.class;
            }
            @Nullable
            @Override
            public Object extractValue(InputRow inputRow, String metricName) {
                throw new UnsupportedOperationException("extractValue without an aggregator factory is not supported.");
            }

            @Nullable
            @Override
            public Object extractValue(InputRow inputRow, String metricName, AggregatorFactory agg) {
                Object rawValue = inputRow.getRaw(metricName);
                if (rawValue instanceof ValueCollection) {
                    return (ValueCollection) rawValue;
                }else if (rawValue instanceof String) {
                    return ValueCollection.deserialize(StringUtils.decodeBase64(StringUtils.toUtf8((String) rawValue)));
                } else {
                    List<String> dimValues = inputRow.getDimension(metricName);
                    if (dimValues == null) {
                        return null;
                    }
                    int v = Double.valueOf(dimValues.get(0).toString()).intValue();
                    return v;
                }
            }
        };
    }

    @Override
    public void deserializeColumn(ByteBuffer buffer, ColumnBuilder columnBuilder)
    {
        final GenericIndexed<ValueCollection> column = GenericIndexed.read(
                buffer,
                this.getObjectStrategy(),
                columnBuilder.getFileMapper()
        );
        columnBuilder.setComplexColumnSupplier(new ComplexColumnPartSupplier(getTypeName(), column));
    }

    @Override
    public GenericColumnSerializer getSerializer(SegmentWriteOutMedium segmentWriteOutMedium, String column)
    {
        return LargeColumnSupportedComplexColumnSerializer.create(
                segmentWriteOutMedium,
                column,
                this.getObjectStrategy()
        );
    }

    @Override
    public ObjectStrategy<ValueCollection> getObjectStrategy(){
        return STRATEGY;
    }
}