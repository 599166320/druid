/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.timeline.partition;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.BoundType;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import org.apache.commons.lang.StringUtils;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.Rows;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.java.util.emitter.EmittingLogger;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class KafkaPartitionNumberedShardSpec extends NumberedShardSpec
{
  private static final EmittingLogger log = new EmittingLogger(KafkaPartitionNumberedShardSpec.class);
  public static final String TYPE = "kafka_partition";
  private final Set<Integer> kafkaPartitionIds;
  private final int kafkaTotalPartition;
  private final List<String> partitionDimensions;
  private String partitionFunction;
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

  public void setPartitionFunction(String partitionFunction)
  {
    this.partitionFunction = partitionFunction;
  }

  @JsonCreator
  public KafkaPartitionNumberedShardSpec(
      @JsonProperty("partitionNum") int partitionNum,
      // partitionId
      @JsonProperty("partitions") int partitions,
      // core partition set size
      @JsonProperty("kafkaPartitionIds") @Nullable Set<Integer> kafkaPartitionIds,
      // nullable for backward compatibility
      @JsonProperty("kafkaTotalPartition") @Nullable Integer kafkaTotalPartition,
      // nullable for backward compatibility
      @JsonProperty("partitionDimensions") @Nullable List<String> partitionDimensions,
      @JsonProperty("partitionFunction") @Nullable String partitionFunction,
      // nullable for backward compatibility
      @JsonProperty("fixedPartitionEnd") @Nullable Integer fixedPartitionEnd,
      // nullable for backward compatibility
      @JacksonInject ObjectMapper jsonMapper
  )
  {
    super(partitionNum, partitions);
    this.kafkaPartitionIds = kafkaPartitionIds;
    this.kafkaTotalPartition = kafkaTotalPartition;
    this.partitionDimensions = partitionDimensions;
    this.partitionFunction = partitionFunction;
    int p = 0;
    if (StringUtils.isNotBlank(partitionFunction) && partitionFunction.length() >= 5) {
      try {
        this.partitionFunction = updateKafkaTotalPartition(kafkaPartitionIds, this.partitionFunction);
        MetricsRtCustomPartitionsConf metricsRtCustomPartitionsConf = MetricsRtCustomPartitionsConf.newMetricsRtCustomPartitionsConf(
            partitionFunction);
        p = metricsRtCustomPartitionsConf.getCustomPartitionNum();
      }
      catch (Exception e) {
        log.warn("fail to parse MetricsRtCustomPartitionsConf:" + partitionFunction);
      }
    }
    //必须最后设置
    this.fixedPartitionEnd = p;
    this.jsonMapper = jsonMapper;
  }

  public String updateKafkaTotalPartition(Set<Integer> partitionIdTypeSet, String partitionFunction)
      throws JsonProcessingException
  {
    if (partitionIdTypeSet.size() == 0) {
      return partitionFunction;
    }
    KafkaPartitionNumberedShardSpec.MetricsRtCustomPartitionsConf conf;
    try {
      conf = KafkaPartitionNumberedShardSpec.MetricsRtCustomPartitionsConf.newMetricsRtCustomPartitionsConf(
          partitionFunction);
    }
    catch (JsonProcessingException e) {
      throw e;
    }
    //分区描述信息太大，做裁剪
    Set<String> removeKeys = new HashSet<>();
    for (Map.Entry<String, Integer> e : conf.getPartitionMap().entrySet()) {
      if (!partitionIdTypeSet.contains(e.getValue())) {
        removeKeys.add(e.getKey());
      }
    }
    for (String key : removeKeys) {
      conf.getPartitionMap().remove(key);
    }
    removeKeys.clear();

    for (Map.Entry<String, Set<Integer>> e : conf.getDataSkewMap().entrySet()) {
      Set<Integer> s1 = new HashSet<Integer>(e.getValue());
      Set<Integer> s2 = new HashSet<Integer>(partitionIdTypeSet);
      s1.retainAll(s2);
      if (s1.size() == 0) {
        removeKeys.add(e.getKey());
      }
    }
    for (String key : removeKeys) {
      conf.getDataSkewMap().remove(key);
    }
    removeKeys.clear();
    if (Objects.isNull(conf.getRandomWriteList())) {
      conf.setRandomWriteList(new HashSet<>());
    }
    return JacksonUtils.JSON_MAPPER.writeValueAsString(conf);
  }

  private boolean notKafkaPartitionNumberShardSpec()
  {
    return Objects.isNull(kafkaPartitionIds)
           || kafkaTotalPartition <= 0
           || Objects.isNull(partitionDimensions)
           || StringUtils.isEmpty(partitionFunction)
           || partitionFunction.length() < 5;
  }

  @Override
  public ShardSpecLookup getLookup(final List<? extends ShardSpec> shardSpecs)
  {
    if (notKafkaPartitionNumberShardSpec()) {
      return super.getLookup(shardSpecs);
    }
    return (long timestamp, InputRow row) -> {
      int index = hash(timestamp, row) + fixedPartitionEnd;
      return shardSpecs.get(index);
    };
  }

  @Override
  public List<String> getDomainDimensions()
  {
    if (notKafkaPartitionNumberShardSpec()) {
      return super.getDomainDimensions();
    }
    return partitionDimensions;
  }

  @JsonProperty
  public int getFixedPartitionEnd()
  {
    return fixedPartitionEnd;
  }


  @Override
  public boolean possibleInDomain(Map<String, RangeSet<String>> domain)
  {
    if (notKafkaPartitionNumberShardSpec()) {
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


  @Override
  public boolean checkForcePartition()
  {
    return true;
  }

  @Override
  public boolean forcePartition(Set<String> partitionIds)
  {
    if (partitionIds != null) {
      for (String p : partitionIds) {
        for (Integer id : kafkaPartitionIds) {
          boolean eq = p.equals(id + "");
          if (eq) {
            return true;
          }
        }
      }
    }
    return super.forcePartition(partitionIds);
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
    ).stream().map(o -> o.toString()).collect(Collectors.toList());
    //由于存在热点指标,除了考虑hash分区外还要考虑枚举分区,枚举分区需要使用groupKey查询nacos
    String kjoin = String.join(",", groupKey);
    String partitionLog = "";
    try {
      partitionLog = "当前检查的segment分区列表是:" + JacksonUtils.JSON_MAPPER.writeValueAsString(kafkaPartitionIds);
      MetricsRtCustomPartitionsConf metricsRtCustomPartitionsConf = MetricsRtCustomPartitionsConf.newMetricsRtCustomPartitionsConf(
          partitionFunction);
      if (metricsRtCustomPartitionsConf.inRandomWriteList(kjoin)) {
        //随机写不做任何优化
        return true;
      }
      int status = metricsRtCustomPartitionsConf.fixPartition(kjoin, kafkaPartitionIds);
      log.debug("固定分区检查key:" + kjoin + ",检查结果是:" + status + "," + partitionLog);
      if (status != 2) {
        return status == 1 ? true : false;
      }
    }
    catch (Exception e) {
      log.error("计算固定分区异常:" + e.getMessage());
    }
    int hashValue = hash(serializeGroupKey(JacksonUtils.JSON_MAPPER, groupKey)) + fixedPartitionEnd;
    log.debug("hash分区检查key:" + kjoin + ",检查结果是:" + hashValue + "," + partitionLog);
    return kafkaPartitionIds.contains(hashValue);
  }

  public static byte[] serializeGroupKey(ObjectMapper jsonMapper, List<String> partitionKeys)
  {
    try {
      //return jsonMapper.writeValueAsBytes(partitionKeys);
      return String.join(",", partitionKeys).getBytes(StandardCharsets.UTF_8);
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
        kafkaTotalPartition - fixedPartitionEnd
    );
  }

  @VisibleForTesting
  Integer hash(final long timestamp, final InputRow inputRow)
  {
    return HashPartitionFunction.MURMUR3_32_ABS.hash(
        HashBasedNumberedShardSpec.serializeGroupKey(JacksonUtils.JSON_MAPPER, extractKeys(timestamp, inputRow)),
        kafkaTotalPartition - fixedPartitionEnd
    );
  }

  /**
   * This method extracts keys for hash partitioning based on whether {@param partitionDimensions} is empty or not.
   * If yes, then both {@param timestamp} and dimension values in {@param inputRow} are returned.
   * Otherwise, values of {@param partitionDimensions} are returned.
   *
   * @param timestamp should be bucketed with query granularity
   * @param inputRow  row from input data
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

  public static class MetricsRtCustomPartitionsConf
  {
    private Map<String, Integer> partitionMap;
    private int partitionNum;
    private int customPartitionNum;
    private Map<String, Set<Integer>> dataSkewMap;
    private String partitionDimensions;
    private Set<String> randomWriteList;

    @JsonCreator
    public MetricsRtCustomPartitionsConf(
        @JsonProperty("partitionMap") Map<String, Integer> partitionMap,
        @JsonProperty("partitionNum") int partitionNum,
        @JsonProperty("customPartitionNum") int customPartitionNum,
        @JsonProperty("dataSkewMap") Map<String, Set<Integer>> dataSkewMap,
        @JsonProperty("partitionDimensions") String partitionDimensions,
        @JsonProperty("randomWriteList") Set<String> randomWriteList
    )
    {
      this.partitionMap = partitionMap;
      this.partitionNum = partitionNum;
      this.customPartitionNum = customPartitionNum;
      this.dataSkewMap = dataSkewMap;
      this.partitionDimensions = partitionDimensions;
      this.randomWriteList = randomWriteList;
    }


    public static MetricsRtCustomPartitionsConf newMetricsRtCustomPartitionsConf(String partitionFunction)
        throws JsonProcessingException
    {
      return JacksonUtils.JSON_MAPPER.readValue(partitionFunction, MetricsRtCustomPartitionsConf.class);
    }


    public String getPartitionDimensions()
    {
      return partitionDimensions;
    }

    public void setPartitionDimensions(String partitionDimensions)
    {
      this.partitionDimensions = partitionDimensions;
    }

    public int getPartitionNum()
    {
      return partitionNum;
    }

    public void setPartitionNum(int partitionNum)
    {
      this.partitionNum = partitionNum;
    }

    public int getCustomPartitionNum()
    {
      return customPartitionNum;
    }

    public void setCustomPartitionNum(int customPartitionNum)
    {
      this.customPartitionNum = customPartitionNum;
    }

    public Map<String, Integer> getPartitionMap()
    {
      return partitionMap;
    }

    public void setPartitionMap(Map<String, Integer> partitionMap)
    {
      this.partitionMap = partitionMap;
    }

    public Map<String, Set<Integer>> getDataSkewMap()
    {
      return dataSkewMap;
    }

    public void setDataSkewMap(Map<String, Set<Integer>> dataSkewMap)
    {
      this.dataSkewMap = dataSkewMap;
    }

    public Set<String> getRandomWriteList()
    {
      return randomWriteList;
    }

    public void setRandomWriteList(Set<String> randomWriteList)
    {
      this.randomWriteList = randomWriteList;
    }

    public boolean inRandomWriteList(String values)
    {
      return randomWriteList.contains(values);
    }

    /**
     * @param values
     * @param kafkaPartitionIds
     * @return 0:明确不存在，1:明确不存在,2:不明确是否存在
     */
    public int fixPartition(String values, Set<Integer> kafkaPartitionIds)
    {
      try {
        if (dataSkewMap.containsKey(values)) {
          Set<Integer> partitionSet = dataSkewMap.get(values);
          for (Integer id : kafkaPartitionIds) {
            if (partitionSet.contains(id)) {
              return 1;
            }
          }
          return 0;
        }
        values = getMD5(values);
        if (partitionMap.containsKey(values)) {
          return kafkaPartitionIds.contains(partitionMap.get(values)) ? 1 : 0;
        }
      }
      catch (Exception e) {
        log.error("分区查询异常,values=" + values + ",kafkaPartitionIds=" + kafkaPartitionIds);
      }
      return 2;
    }

    public static String getMD5(String source) throws Exception
    {
      return getMD5(source.getBytes(StandardCharsets.UTF_8));
    }

    public static String getMD5(byte[] source) throws Exception
    {
      String s;
      char[] hexDigits = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
      try {
        MessageDigest md = MessageDigest.getInstance("MD5");
        md.update(source);
        byte[] tmp = md.digest();
        char[] str = new char[16 * 2];
        int k = 0;
        for (int i = 0; i < 16; i++) {
          byte byte0 = tmp[i];
          str[k++] = hexDigits[byte0 >>> 4 & 0xf];
          str[k++] = hexDigits[byte0 & 0xf];
        }
        s = new String(str);

      }
      catch (Exception e) {
        throw e;
      }
      return s;
    }
  }
}
