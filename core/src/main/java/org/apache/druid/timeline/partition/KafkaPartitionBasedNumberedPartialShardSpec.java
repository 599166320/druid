package org.apache.druid.timeline.partition;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class KafkaPartitionBasedNumberedPartialShardSpec implements PartialShardSpec{
    public static final String TYPE = "kafka_partition";
    @Nullable
    private final List<String> partitionDimensions;
    private final Set<Integer> kafkaPartitionIds;
    private final int kafkaTotalPartition;
    @Nullable
    private final String partitionFunction;

    @JsonCreator
    public KafkaPartitionBasedNumberedPartialShardSpec(
            @JsonProperty("partitionDimensions") @Nullable List<String> partitionDimensions,
            @JsonProperty("kafkaPartitionIds") Set<Integer> kafkaPartitionIds,
            @JsonProperty("kafkaTotalPartition") int kafkaTotalPartition,
            @JsonProperty("partitionFunction") @Nullable String partitionFunction // nullable for backward compatibility
    ){
        this.partitionDimensions = partitionDimensions;
        this.kafkaPartitionIds = kafkaPartitionIds;
        this.kafkaTotalPartition = kafkaTotalPartition;
        this.partitionFunction = partitionFunction;
    }

    @Nullable
    @JsonProperty
    public List<String> getPartitionDimensions()
    {
        return partitionDimensions;
    }

    @JsonProperty
    public Set<Integer> getkafkaPartitionIds()
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

    @Override
    public ShardSpec complete(ObjectMapper objectMapper, int partitionId, int numCorePartitions)
    {
        return new KafkaPartitionNumberedShardSpec(
                partitionId,
                numCorePartitions,
                kafkaPartitionIds,
                kafkaTotalPartition,
                partitionDimensions,
                partitionFunction,
                objectMapper
        );
    }

    @Override
    public Class<? extends ShardSpec> getShardSpecClass()
    {
        return KafkaPartitionNumberedShardSpec.class;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        KafkaPartitionBasedNumberedPartialShardSpec that = (KafkaPartitionBasedNumberedPartialShardSpec) o;
        return Objects.equals(kafkaPartitionIds, that.kafkaPartitionIds) &&
                kafkaTotalPartition == that.kafkaTotalPartition &&
                Objects.equals(partitionDimensions, that.partitionDimensions) &&
                Objects.equals(partitionFunction, that.partitionFunction);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(partitionDimensions, kafkaPartitionIds, kafkaTotalPartition, partitionFunction);
    }
}
