package org.apache.druid.query.aggregation.quantile;
import com.fasterxml.jackson.databind.ObjectMapper;
import me.lemire.integercompression.differential.IntegratedIntCompressor;
import org.apache.druid.collections.CloseableStupidPool;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.guice.QuantileExactExtensionModule;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.StringUtils;
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
import org.roaringbitmap.IntConsumer;
import org.roaringbitmap.RoaringBitmap;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

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
    public void compress(){
        ValueCollection xx  =ValueCollection.deserialize(StringUtils.decodeBase64(StringUtils.toUtf8("AAAA5iAgICAAAAYBAAAAAQAABgMAAAABAAAGBAAAAAEAAAYIAAAAAQAABgoAAAABAAAGDAAAAAIAAAYNAAAAAgAABg8AAAABAAAGEAAAAAIAAAYUAAAAAQAABhoAAAABAAAFGgAAAAEAAAYbAAAAAgAABiEAAAABAAAGIwAAAAMAAAYkAAAAAgAABicAAAACAAAGKQAAAAEAAAUsAAAAAQAABiwAAAABAAAGLgAAAAEAAAYyAAAAAwAABTMAAAABAAAGNwAAAAEAAAY/AAAAAQAABUMAAAABAAAGRAAAAAEAAAZFAAAAAgAABUcAAAABAAAGSQAAAAEAAAVJAAAAAQAABUsAAAACAAAGSwAAAAEAAAZOAAAAAQAABVMAAAABAAAGVAAAAAEAAAVUAAAAAQAABVUAAAABAAAFVgAAAAEAAAZbAAAAAQAABVwAAAABAAAFZAAAAAIAAAZlAAAAAQAABWkAAAABAAAFbQAAAAEAAAV0AAAAAQAABXUAAAACAAAFegAAAAEAAAV8AAAAAgAABX8AAAACAAAFgAAAAAEAAAWCAAAAAQAABYQAAAABAAAFhQAAAAIAAAWHAAAAAwAABYoAAAABAAAFjQAAAAEAAAWRAAAAAQAABZQAAAAEAAAFlQAAAAEAAAWWAAAAAgAABZcAAAABAAAFmAAAAAEAAAWaAAAAAQAAACAAAAWfAAAAAQAABaEAAAACAAAFpAAAAAEAAAWlAAAAAgAABaYAAAACAAAFpwAAAAEAAAWpAAAAAgAABaoAAAABAAAFrAAAAAIAAAWvAAAAAQAABbAAAAABAAAFsQAAAAEAAAWyAAAAAQAABbMAAAACAAAFugAAAAEAAAW9AAAAAgAAACAAAAW/AAAAAgAABcEAAAABAAAFwwAAAAEAAAXFAAAAAQAABcYAAAABAAAFxwAAAAEAAAXJAAAAAwAABcoAAAABAAAFywAAAAEAAAXMAAAAAQAABc4AAAABAAAF0AAAAAEAAAXRAAAAAgAABdMAAAADAAAF1gAAAAEAAAXXAAAAAQAAACAAAAXZAAAAAgAABd0AAAABAAAF3wAAAAIAAAXgAAAAAQAABeEAAAABAAAF5AAAAAEAAAXnAAAAAQAABegAAAABAAAF6QAAAAEAAAXqAAAAAQAABe0AAAACAAAF7gAAAAEAAAXvAAAAAQAABfIAAAACAAAF9gAAAAIAAAX4AAAAAXQFi3t8j39/f3QFi4t8j39/f3QDAAAAjw==")));

        System.out.println(xx);

        IntegratedIntCompressor iic = new IntegratedIntCompressor();
        int[] data = {100,2,200,23,38,56,7,100,2,3,99,0,128,20348,123,56,777};
        System.out.println("原始数据:"+data.length);
        int[] compressed = iic.compress(data); // compressed array
        System.out.println("压缩数据:"+compressed.length);
        int[] recov = iic.uncompress(compressed); // equals to data
        System.out.println("解压数据:"+recov.length);
    }

    @Test
    public void mem(){

        CloseableStupidPool<ByteBuffer> stupidPool = new CloseableStupidPool<>(
                "QueryRunners-bufferPool",
                () -> ByteBuffer.allocate(1024 * 1024 * 10)
        );

        ResourceHolder<ByteBuffer> resourceHolder = stupidPool.take();
        ByteBuffer buffer = resourceHolder.get();
        for(int i = 0;i<100;i++){
            buffer.putInt(i);
        }

        buffer.flip();
        while (buffer.hasRemaining()){
            System.out.println(buffer.getInt());
        }
        System.out.println(buffer.hashCode());
        resourceHolder.close();

        //复用
        resourceHolder = stupidPool.take();
        buffer = resourceHolder.get();
        buffer.clear();
        for(int i = 0;i<100;i++){
            buffer.putInt(i);
        }

        buffer.flip();
        while (buffer.hasRemaining()){
            System.out.println(buffer.getInt());
        }
        System.out.println(buffer.hashCode());
        resourceHolder.close();
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


    @Test
    public void test1(){
        // 向r1中添加1、2、3、1000四个数字
        RoaringBitmap r1 = RoaringBitmap.bitmapOf(1, 2, 3, 1000);
        // 返回第3个数字是1000
        System.out.println(r1.select(3));

        r1.add(5);

        // 返回10000的索引,是4
        System.out.println(r1.rank(1000));
        System.out.println(r1.rank(3));
        System.out.println(r1.rank(2));
        System.out.println(r1.rank(1));

        // 是否包含1000和7,true和false
        System.out.println(r1.contains(1000));
        System.out.println(r1.contains(7));

        RoaringBitmap r2 = new RoaringBitmap();
        // 向r2添加10000-12000共2000个数字
        r2.add(10000L, 12000L);

        // 将两个r1,r2进行合并,数值进行合并,合并产生新的RoaringBitmap
        RoaringBitmap r3 = RoaringBitmap.or(r1, r2);

        // r1和r2进行位运算,并将结果赋值给r1
        r1.or(r2);

        // 判断r1与r3是否相等,true
        System.out.println(r1.equals(r3));

        // 查看r1中存储了多少个值,2004
        System.out.println(r1.getLongCardinality());

        // 两种遍历方式
        for(int i : r1){
            System.out.println(i);
        }

        r1.forEach((Consumer<? super Integer>) i -> System.out.println(i.intValue()));
    }

    @Test
    public void test2(){
        RoaringBitmap roaringBitmap = new RoaringBitmap();
        roaringBitmap.add(1L, 10L);

        // 遍历输出
        roaringBitmap.forEach((IntConsumer) i -> System.out.println(i));

        // 遍历放入List中
        List<Integer> numbers = new ArrayList<>();
        roaringBitmap.forEach((IntConsumer) numbers::add);
        System.out.println(numbers);

        roaringBitmap.runOptimize();

        int size = roaringBitmap.serializedSizeInBytes();
        ByteBuffer buffer = ByteBuffer.allocate(size);
        roaringBitmap.serialize(buffer);
        // 将RoaringBitmap的数据转成字节数组,这样就可以直接存入数据库了,数据库字段类型BLOB
        byte[] bitmapData = buffer.array();
    }
}