package org.apache.druid.query.aggregation.quantile;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.guice.QuantileExactExtensionModule;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.aggregation.AggregationTestHelper;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByQueryRunnerTest;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
@RunWith(Parameterized.class)
public class ValueAppendAggregatorTest extends InitializedNullHandlingTest
{
    private final AggregationTestHelper helper;

    @Rule
    public final TemporaryFolder tempFolder = new TemporaryFolder();

    public ValueAppendAggregatorTest(final GroupByQueryConfig config)
    {
        QuantileExactExtensionModule module = new QuantileExactExtensionModule();
        helper = AggregationTestHelper.createGroupByQueryAggregationTestHelper(
                module.getJacksonModules(), config, tempFolder);
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<?> constructorFeeder()
    {
        final List<Object[]> constructors = new ArrayList<>();
        for (GroupByQueryConfig config : GroupByQueryRunnerTest.testConfigs()) {
            constructors.add(new Object[]{config});
        }
        return constructors;
    }

    @Test
    public void testBuff(){
        List<Double> values = new ArrayList<>();
        values.add(1.0);
        values.add(2.0);
        values.add(3.0);
        values.add(1.0);
        values.add(1.0);
        values.add(1.0);
        ByteBuffer buffer = ByteBuffer.allocate(Double.BYTES*values.size());
        for(Double v:values){
            buffer.putDouble(v);
        }
        byte[] bs = buffer.array();
        buffer.flip();
        while(buffer.hasRemaining()){
            System.out.println(buffer.getDouble());
        }
        buffer = ByteBuffer.wrap(bs);
        while(buffer.hasRemaining()){
            System.out.println(buffer.getDouble());
        }
    }


    // this is to test Json properties and equals
    @Test
    public void serializeDeserializeFactoryWithFieldName() throws Exception
    {
        ObjectMapper objectMapper = new DefaultObjectMapper();
        new QuantileExactExtensionModule().getJacksonModules().forEach(objectMapper::registerModule);
        ValueAppendAggregatorFactory factory = new ValueAppendAggregatorFactory("name", "filedName", 1,"max");
        AggregatorFactory other = objectMapper.readValue(
                objectMapper.writeValueAsString(factory),
                AggregatorFactory.class
        );
        Assert.assertEquals(factory, other);
    }

    @Test
    public void buildingSketchesAtIngestionTime() throws Exception
    {
        Sequence<ResultRow> seq = helper.createIndexAndRunQueryOnSegment(
                new File(this.getClass().getClassLoader().getResource("doubles_build_data.tsv").getFile()),
                String.join(
                        "\n",
                        "{",
                        "  \"type\": \"string\",",
                        "  \"parseSpec\": {",
                        "    \"format\": \"tsv\",",
                        "    \"timestampSpec\": {\"column\": \"timestamp\", \"format\": \"yyyyMMddHHmmss\"},",
                        "    \"dimensionsSpec\": {",
                        "      \"dimensions\": [\"sequenceNumber\",\"product\"],",
                        "      \"dimensionExclusions\": [ ],",
                        "      \"spatialDimensions\": []",
                        "    },",
                        "    \"columns\": [\"timestamp\", \"sequenceNumber\", \"product\", \"value\"]",
                        "  }",
                        "}"
                ),
                "[{\"type\": \"valueAppend\", \"name\": \"value\", \"fieldName\": \"value\", \"maxIntermediateSize\": -1}]",
                //   "[]",,k,
                0, // minTimestamp
                Granularities.HOUR,
                10000, // maxRowCount

                String.join(
                        "\n",
                        "{",
                        "  \"queryType\": \"groupBy\",",
                        "  \"dataSource\": \"test_datasource\",",
                        "  \"granularity\": \"HOUR\",",
                        "  \"dimensions\": [\"sequenceNumber\",\"product\"],",
                        "  \"aggregations\": [",
                        "{\"type\": \"valueAppend\", \"name\": \"value\", \"fieldName\": \"value\",\"maxIntermediateSize\": -1} "+
                        //"{\"type\": \"valueAppend\", \"name\": \"value\", \"fieldName\": \"value\",\"maxIntermediateSize\": -1}, "+
                               // "{\"type\": \"count\", \"name\": \"a\"}",
                        "  ],",
                        "  \"postAggregations\": [",
                        "  ],",
                        "  \"intervals\": [\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"]",
                        "}"
                )
        );
        List<ResultRow> results = seq.toList();


        ResultRow row = results.get(0);
        ArrayList<Double> tmp = (ArrayList) row.get(2);
        for(Double v : tmp){
            System.out.println(v);
        }
        row = results.get(1);
        tmp = (ArrayList<Double>) row.get(2);
        for(Double v : tmp){
            System.out.println(v);
        }
        Assert.assertEquals(1, results.size());
        // post agg
        Object quantilesObject = row.get(1); // "quantiles"
        Assert.assertTrue(quantilesObject instanceof double[]);
        double[] quantiles = (double[]) quantilesObject;
        Assert.assertEquals(0.001, quantiles[0], 0.0006); // min value
        Assert.assertEquals(NullHandling.replaceWithDefault() ? 0.47 : 0.5, quantiles[1], 0.05); // median value
        Assert.assertEquals(1, quantiles[2], 0.05); // max value
    }

    @Test
    public void buildingSketchesAtQueryTime() throws Exception
    {
        Sequence<ResultRow> seq = helper.createIndexAndRunQueryOnSegment(
                new File(this.getClass().getClassLoader().getResource("doubles_build_data.tsv").getFile()),
                String.join(
                        "\n",
                        "{",
                        "  \"type\": \"string\",",
                        "  \"parseSpec\": {",
                        "    \"format\": \"tsv\",",
                        "    \"timestampSpec\": {\"column\": \"timestamp\", \"format\": \"yyyyMMddHH\"},",
                        "    \"dimensionsSpec\": {",
                        "      \"dimensions\": [\"sequenceNumber\", \"product\"],",
                        "      \"dimensionExclusions\": [],",
                        "      \"spatialDimensions\": []",
                        "    },",
                        "    \"columns\": [\"timestamp\", \"sequenceNumber\", \"product\", \"value\"]",
                        "  }",
                        "}"
                ),
                "[{\"type\": \"doubleSum\", \"name\": \"value\", \"fieldName\": \"value\"}]",
                0, // minTimestamp
                Granularities.NONE,
                10, // maxRowCount
                String.join(
                        "\n",
                        "{",
                        "  \"queryType\": \"groupBy\",",
                        "  \"dataSource\": \"test_datasource\",",
                        "  \"granularity\": \"ALL\",",
                        "  \"dimensions\": [],",
                        "  \"aggregations\": [",
                        "    {\"type\": \"tDigestSketch\", \"name\": \"sketch\", \"fieldName\": \"value\", \"compression\": 200}",
                        "  ],",
                        "  \"postAggregations\": [",
                        "    {\"type\": \"quantilesFromTDigestSketch\", \"name\": \"quantiles\", \"fractions\": [0, 0.5, 1], \"field\": {\"type\": \"fieldAccess\", \"fieldName\": \"sketch\"}}",
                        "  ],",
                        "  \"intervals\": [\"2016-01-01T00:00:00.000Z/2016-01-31T00:00:00.000Z\"]",
                        "}"
                )
        );
        List<ResultRow> results = seq.toList();
        Assert.assertEquals(1, results.size());
        ResultRow row = results.get(0);


        // post agg
        Object quantilesObject = row.get(1); // "quantiles"
        Assert.assertTrue(quantilesObject instanceof double[]);
        double[] quantiles = (double[]) quantilesObject;
        Assert.assertEquals(NullHandling.replaceWithDefault() ? 0.0 : 0.001, quantiles[0], 0.0006); // min value
        Assert.assertEquals(NullHandling.replaceWithDefault() ? 0.35 : 0.5, quantiles[1], 0.05); // median value
        Assert.assertEquals(1, quantiles[2], 0.05); // max value
    }

    @Test
    public void testIngestingSketches() throws Exception
    {
        Sequence<ResultRow> seq = helper.createIndexAndRunQueryOnSegment(
                new File(this.getClass().getClassLoader().getResource("doubles_sketch_data.tsv").getFile()),
                String.join(
                        "\n",
                        "{",
                        "  \"type\": \"string\",",
                        "  \"parseSpec\": {",
                        "    \"format\": \"tsv\",",
                        "    \"timestampSpec\": {\"column\": \"timestamp\", \"format\": \"yyyyMMddHH\"},",
                        "    \"dimensionsSpec\": {",
                        "      \"dimensions\": [\"product\"],",
                        "      \"dimensionExclusions\": [],",
                        "      \"spatialDimensions\": []",
                        "    },",
                        "    \"columns\": [\"timestamp\", \"product\", \"sketch\"]",
                        "  }",
                        "}"
                ),
                String.join(
                        "\n",
                        "[",
                        "  {\"type\": \"tDigestSketch\", \"name\": \"first_level_merge_sketch\", \"fieldName\": \"sketch\", "
                                + "\"compression\": "
                                + "200}",
                        "]"
                ),
                0, // minTimestamp
                Granularities.NONE,
                10, // maxRowCount
                String.join(
                        "\n",
                        "{",
                        "  \"queryType\": \"groupBy\",",
                        "  \"dataSource\": \"test_datasource\",",
                        "  \"granularity\": \"ALL\",",
                        "  \"dimensions\": [],",
                        "  \"aggregations\": [",
                        "    {\"type\": \"tDigestSketch\", \"name\": \"second_level_merge_sketch\", \"fieldName\": "
                                + "\"first_level_merge_sketch\", \"compression\": "
                                + "200}",
                        "  ],",
                        "  \"postAggregations\": [",
                        "    {\"type\": \"quantilesFromTDigestSketch\", \"name\": \"quantiles\", \"fractions\": [0, 0.5, 1], \"field\": {\"type\": \"fieldAccess\", \"fieldName\": \"second_level_merge_sketch\"}}",
                        "  ],",
                        "  \"intervals\": [\"2016-01-01T00:00:00.000Z/2016-01-31T00:00:00.000Z\"]",
                        "}"
                )
        );
        List<ResultRow> results = seq.toList();
        Assert.assertEquals(1, results.size());
        ResultRow row = results.get(0);

        // post agg
        Object quantilesObject = row.get(1); // "quantiles"
        Assert.assertTrue(quantilesObject instanceof double[]);
        double[] quantiles = (double[]) quantilesObject;
        Assert.assertEquals(0.001, quantiles[0], 0.0006); // min value
        Assert.assertEquals(NullHandling.replaceWithDefault() ? 0.47 : 0.5, quantiles[1], 0.05); // median value
        Assert.assertEquals(1, quantiles[2], 0.05); // max value
    }
}