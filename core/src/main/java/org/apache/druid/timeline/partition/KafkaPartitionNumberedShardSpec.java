package org.apache.druid.timeline.partition;
import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.BoundType;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import org.apache.commons.lang.StringUtils;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.Rows;
import org.apache.druid.java.util.common.ISE;

import javax.annotation.Nullable;
import java.util.*;

public class KafkaPartitionNumberedShardSpec extends NumberedShardSpec
{
    public static final String TYPE = "kafka_partition";
    private final Set<Integer> kafkaPartitionIds;
    private final int kafkaTotalPartition;
    private final List<String> partitionDimensions;
    private final String partitionFunction;
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
            @JacksonInject ObjectMapper jsonMapper
    )
    {
        super(partitionNum, partitions);
        this.kafkaPartitionIds = kafkaPartitionIds;
        this.kafkaTotalPartition = kafkaTotalPartition;
        this.partitionDimensions = partitionDimensions;
        this.partitionFunction = partitionFunction;
        this.jsonMapper = jsonMapper;
    }

    private boolean isKafkaPartitionNUmberShardSpec(){
        return Objects.isNull(kafkaPartitionIds) || kafkaTotalPartition <= 0 || Objects.isNull(partitionDimensions) || StringUtils.isEmpty(partitionFunction);
    }

    @Override
    public ShardSpecLookup getLookup(final List<? extends ShardSpec> shardSpecs)
    {
        if(isKafkaPartitionNUmberShardSpec()){
            return super.getLookup(shardSpecs);
        }
        return (long timestamp, InputRow row) -> {
            int index = hash(timestamp, row);
            return shardSpecs.get(index);
        };
    }

    @Override
    public List<String> getDomainDimensions() {
        if (isKafkaPartitionNUmberShardSpec()){
            return super.getDomainDimensions();
        }
        return partitionDimensions;
    }

    @Override
    public boolean possibleInDomain(Map<String, RangeSet<String>> domain) {
        if (isKafkaPartitionNUmberShardSpec()){
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
        List<Object> groupKey = Lists.transform(
                partitionDimensions,
                o -> Collections.singletonList(partitionDimensionsValues.get(o))
        );
        //由于存在热点指标,除了考虑hash分区外还要考虑枚举分区,枚举分区需要使用groupKey查询nacos
        int hashValue = hash(serializeGroupKey(jsonMapper, groupKey));
        return  kafkaPartitionIds.contains(hashValue);
    }

    public static byte[] serializeGroupKey(ObjectMapper jsonMapper, List<Object> partitionKeys)
    {
        try {
            return jsonMapper.writeValueAsBytes(partitionKeys);
        }
        catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
    @VisibleForTesting
    Integer hash(byte[] data)
    {
        return HashPartitionFunction.MURMUR3_32_ABS.hash(
                data,
                kafkaTotalPartition
        );
    }
    @VisibleForTesting
    Integer hash(final long timestamp, final InputRow inputRow)
    {
        return HashPartitionFunction.MURMUR3_32_ABS.hash(
                HashBasedNumberedShardSpec.serializeGroupKey(jsonMapper, extractKeys(timestamp, inputRow)),
                kafkaTotalPartition
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
}
