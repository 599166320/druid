package org.apache.druid.timeline.partition;
import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.BoundType;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import org.apache.commons.lang.StringUtils;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.Rows;
import org.apache.druid.java.util.emitter.EmittingLogger;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

public class KafkaPartitionNumberedShardSpec extends NumberedShardSpec
{
    private static final EmittingLogger log = new EmittingLogger(KafkaPartitionNumberedShardSpec.class);
    public static final String TYPE = "kafka_partition";
    private final Set<Integer> kafkaPartitionIds;
    private final int kafkaTotalPartition;
    private final List<String> partitionDimensions;
    private final String partitionFunction;
    private final int fixedPartitionEnd;
    private final ObjectMapper jsonMapper;

    @Nullable
    @JsonProperty
    public List<String> getPartitionDimensions()
    {
        return partitionDimensions;
    }

    @JsonProperty
    public Set<Integer> getKafkaPartitionIds()
    {
        return kafkaPartitionIds;
    }

    @JsonProperty
    public int getKafkaTotalPartition()
    {
        return kafkaTotalPartition;
    }

    @JsonProperty
    @Nullable
    public String getPartitionFunction()
    {
        return partitionFunction;
    }

    @JsonCreator
    public KafkaPartitionNumberedShardSpec(
            @JsonProperty("partitionNum") int partitionNum,    // partitionId
            @JsonProperty("partitions") int partitions,        // core partition set size
            @JsonProperty("kafkaPartitionIds") @Nullable Set<Integer> kafkaPartitionIds, // nullable for backward compatibility
            @JsonProperty("kafkaTotalPartition") @Nullable Integer kafkaTotalPartition, // nullable for backward compatibility
            @JsonProperty("partitionDimensions") @Nullable List<String> partitionDimensions,
            @JsonProperty("partitionFunction") @Nullable String partitionFunction, // nullable for backward compatibility
            @JsonProperty("fixedPartitionEnd") @Nullable Integer fixedPartitionEnd, // nullable for backward compatibility
            @JacksonInject ObjectMapper jsonMapper
    )
    {
        super(partitionNum, partitions);
        this.kafkaPartitionIds = kafkaPartitionIds;
        this.kafkaTotalPartition = kafkaTotalPartition;
        this.partitionDimensions = partitionDimensions;
        this.partitionFunction = partitionFunction;
        int p = 0;
        if(StringUtils.isNotBlank(partitionFunction) && partitionFunction.length()>=5){
            try {
                MetricsRtCustomPartitionsConf metricsRtCustomPartitionsConf = jsonMapper.readValue(partitionFunction,MetricsRtCustomPartitionsConf.class);
                p = metricsRtCustomPartitionsConf.getCustomPartitionNum();
            }catch (Exception e){
                log.warn("MetricsRtCustomPartitionsConf解析异常:"+partitionFunction);
            }
        }
        //必须最后设置
        this.fixedPartitionEnd = p;
        this.jsonMapper = jsonMapper;
    }

    private boolean notKafkaPartitionNumberShardSpec(){
        return Objects.isNull(kafkaPartitionIds) || kafkaTotalPartition <= 0 || Objects.isNull(partitionDimensions) || StringUtils.isEmpty(partitionFunction) || partitionFunction.length()<5;
    }

    @Override
    public ShardSpecLookup getLookup(final List<? extends ShardSpec> shardSpecs)
    {
        if(notKafkaPartitionNumberShardSpec()){
            return super.getLookup(shardSpecs);
        }
        return (long timestamp, InputRow row) -> {
            int index = hash(timestamp, row)+fixedPartitionEnd;
            return shardSpecs.get(index);
        };
    }

    @Override
    public List<String> getDomainDimensions() {
        if (notKafkaPartitionNumberShardSpec()){
            return super.getDomainDimensions();
        }
        return partitionDimensions;
    }

    @JsonProperty
    public int getFixedPartitionEnd() {
        return fixedPartitionEnd;
    }


    @Override
    public boolean possibleInDomain(Map<String, RangeSet<String>> domain) {
        if (notKafkaPartitionNumberShardSpec()){
            return super.possibleInDomain(domain);
        }
        Map<String, Set<String>> domainSet = new HashMap<>();
        for (String p : partitionDimensions) {
            RangeSet<String> domainRangeSet = domain.get(p);
            if (domainRangeSet == null || domainRangeSet.isEmpty()) {
                return true;
            }
            for (Range<String> v : domainRangeSet.asRanges()) {
                // If there are range values, simply bypass, because we can't hash range values
                if (v.isEmpty() || !v.hasLowerBound() || !v.hasUpperBound() ||
                        v.lowerBoundType() != BoundType.CLOSED || v.upperBoundType() != BoundType.CLOSED ||
                        !v.lowerEndpoint().equals(v.upperEndpoint())) {
                    return true;
                }
                domainSet.computeIfAbsent(p, k -> new HashSet<>()).add(v.lowerEndpoint());
            }
        }
        return !domainSet.isEmpty() && chunkPossibleInDomain(domainSet, new HashMap<>());
    }

    private boolean chunkPossibleInDomain(
            Map<String, Set<String>> domainSet,
            Map<String, String> partitionDimensionsValues
    )
    {
        int curIndex = partitionDimensionsValues.size();
        if (curIndex == partitionDimensions.size()) {
            return isInChunk(partitionDimensionsValues);
        }

        String dimension = partitionDimensions.get(curIndex);
        for (String e : domainSet.get(dimension)) {
            partitionDimensionsValues.put(dimension, e);
            if (chunkPossibleInDomain(domainSet, partitionDimensionsValues)) {
                return true;
            }
            partitionDimensionsValues.remove(dimension);
        }
        return false;
    }

    private boolean isInChunk(Map<String, String> partitionDimensionsValues)
    {
        assert !partitionDimensions.isEmpty();
        List<String> groupKey = Lists.transform(
                partitionDimensions,
                o -> partitionDimensionsValues.get(o)
        ).stream().map(o->o.toString()).collect(Collectors.toList());
        //由于存在热点指标,除了考虑hash分区外还要考虑枚举分区,枚举分区需要使用groupKey查询nacos
        String kjoin = String.join(",",groupKey);
        String partitionLog = "";
        try {
            partitionLog = "当前检查的segment分区列表是:"+jsonMapper.writeValueAsString(kafkaPartitionIds);
            MetricsRtCustomPartitionsConf metricsRtCustomPartitionsConf = jsonMapper.readValue(partitionFunction,MetricsRtCustomPartitionsConf.class);
            int status = metricsRtCustomPartitionsConf.fixPartition(kjoin,kafkaPartitionIds);
            log.debug("固定分区检查key:"+kjoin+",检查结果是:"+status+","+partitionLog);
            if (status!=2){
                return status==1?true:false;
            }
        }catch (Exception e){
            log.error("计算固定分区异常:"+e.getMessage());
        }
        int hashValue = hash(serializeGroupKey(jsonMapper, groupKey))+fixedPartitionEnd;
        log.debug("hash分区检查key:"+kjoin+",检查结果是:"+hashValue+","+partitionLog);
        return  kafkaPartitionIds.contains(hashValue);
    }

    public static byte[] serializeGroupKey(ObjectMapper jsonMapper, List<String> partitionKeys)
    {
        try {
            //return jsonMapper.writeValueAsBytes(partitionKeys);
            return String.join(",",partitionKeys).getBytes(StandardCharsets.UTF_8);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    @VisibleForTesting
    Integer hash(byte[] data)
    {
        return HashPartitionFunction.MURMUR3_32_ABS.hash(
                data,
                kafkaTotalPartition-fixedPartitionEnd
        );
    }
    @VisibleForTesting
    Integer hash(final long timestamp, final InputRow inputRow)
    {
        return HashPartitionFunction.MURMUR3_32_ABS.hash(
                HashBasedNumberedShardSpec.serializeGroupKey(jsonMapper, extractKeys(timestamp, inputRow)),
                kafkaTotalPartition-fixedPartitionEnd
        );
    }

    /**
     * This method extracts keys for hash partitioning based on whether {@param partitionDimensions} is empty or not.
     * If yes, then both {@param timestamp} and dimension values in {@param inputRow} are returned.
     * Otherwise, values of {@param partitionDimensions} are returned.
     *
     * @param timestamp should be bucketed with query granularity
     * @param inputRow  row from input data
     *
     * @return a list of values of grouping keys
     */
    @VisibleForTesting
    List<Object> extractKeys(final long timestamp, final InputRow inputRow)
    {
        return extractKeys(partitionDimensions, timestamp, inputRow);
    }

    public static List<Object> extractKeys(
            final List<String> partitionDimensions,
            final long timestamp,
            final InputRow inputRow
    )
    {
        if (partitionDimensions.isEmpty()) {
            return Rows.toGroupKey(timestamp, inputRow);
        } else {
            return Lists.transform(partitionDimensions, inputRow::getDimension);
        }
    }

    public static class MetricsRtCustomPartitionsConf{
        private Map<String,Integer> partitionMap;
        private int partitionNum;
        private int customPartitionNum;
        private Map<String,Set<Integer>> dataSkewMap;
        private String partitionDimensions;

        @JsonCreator
        public MetricsRtCustomPartitionsConf(
                @JsonProperty("partitionMap") Map<String,Integer> partitionMap,
                @JsonProperty("partitionNum") int partitionNum,
                @JsonProperty("customPartitionNum") int customPartitionNum,
                @JsonProperty("dataSkewMap") Map<String,Set<Integer>> dataSkewMap,
                @JsonProperty("partitionDimensions") String partitionDimensions
        ){
            this.partitionMap = partitionMap;
            this.partitionNum = partitionNum;
            this.customPartitionNum = customPartitionNum;
            this.dataSkewMap = dataSkewMap;
            this.partitionDimensions = partitionDimensions;
        }

        public String getPartitionDimensions() {
            return partitionDimensions;
        }

        public void setPartitionDimensions(String partitionDimensions) {
            this.partitionDimensions = partitionDimensions;
        }

        public int getPartitionNum() {
            return partitionNum;
        }

        public void setPartitionNum(int partitionNum) {
            this.partitionNum = partitionNum;
        }

        public int getCustomPartitionNum() {
            return customPartitionNum;
        }

        public void setCustomPartitionNum(int customPartitionNum) {
            this.customPartitionNum = customPartitionNum;
        }

        public Map<String, Integer> getPartitionMap() {
            return partitionMap;
        }

        public void setPartitionMap(Map<String, Integer> partitionMap) {
            this.partitionMap = partitionMap;
        }

        public Map<String, Set<Integer>> getDataSkewMap() {
            return dataSkewMap;
        }

        public void setDataSkewMap(Map<String, Set<Integer>> dataSkewMap) {
            this.dataSkewMap = dataSkewMap;
        }

        /**
         *
         * @param values
         * @param kafkaPartitionIds
         * @return 0:明确不存在，1:明确不存在,2:不明确是否存在
         */
        public int fixPartition(String values,Set<Integer> kafkaPartitionIds){
            try {
                if (dataSkewMap.containsKey(values)){
                    Set<Integer> partitionSet = dataSkewMap.get(values);
                    for(Integer id:kafkaPartitionIds){
                        if(partitionSet.contains(id)){
                            return 1;
                        }
                    }
                    return 0;
                }
                values = getMD5(values);
                if (partitionMap.containsKey(values)){
                    return kafkaPartitionIds.contains(partitionMap.get(values))?1:0;
                }
            }catch (Exception e){
                log.error("分区查询异常,values="+values+",kafkaPartitionIds="+kafkaPartitionIds);
            }
            return 2;
        }

        public static String getMD5(String source) throws Exception {
            return getMD5(source.getBytes());
        }

        public static String getMD5(byte[] source) throws Exception {
            String s = null;
            char[] hexDigits = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
            try {
                java.security.MessageDigest md = java.security.MessageDigest.getInstance("MD5");
                md.update(source);
                byte[] tmp = md.digest(); // MD5 的计算结果是一个 128 位的长整数，
                // 用字节表示就是 16 个字节
                char[] str = new char[16 * 2]; // 每个字节用 16 进制表示的话，使用两个字符，
                // 所以表示成 16 进制需要 32 个字符
                int k = 0; // 表示转换结果中对应的字符位置
                for (int i = 0; i < 16; i++) { // 从第一个字节开始，对 MD5 的每一个字节
                    // 转换成 16 进制字符的转换
                    byte byte0 = tmp[i]; // 取第 i 个字节
                    str[k++] = hexDigits[byte0 >>> 4 & 0xf]; // 取字节中高 4 位的数字转换,
                    // >>>
                    // 为逻辑右移，将符号位一起右移
                    str[k++] = hexDigits[byte0 & 0xf]; // 取字节中低 4 位的数字转换
                }
                s = new String(str); // 换后的结果转换为字符串

            } catch (Exception e) {
                throw e;
            }
            return s;
        }
    }

    public static void main(String[] args) throws JsonProcessingException {
        ObjectMapper configMapper = new ObjectMapper();
        String partitionFunction = "{\"partitionDimensions\":\"app,name,cluster\",\"partitionNum\":12,\"customPartitionNum\":0,\"partitionMap\":{},\"dataSkewMap\":{}}";
        KafkaPartitionNumberedShardSpec.MetricsRtCustomPartitionsConf conf = configMapper.copy().configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true).readValue(partitionFunction,KafkaPartitionNumberedShardSpec.MetricsRtCustomPartitionsConf.class);
        System.out.println(conf);
    }
}
