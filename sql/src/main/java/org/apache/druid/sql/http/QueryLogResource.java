package org.apache.druid.sql.http;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.Ordering;
import com.google.inject.Inject;
import com.sun.jersey.spi.container.ResourceFilters;
import org.apache.commons.lang3.StringUtils;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.http.security.RulesResourceFilter;
import org.apache.druid.server.log.RequestLogger;
import org.apache.druid.sql.SqlLifecycleManager;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Path("/druid/v2/query/")
public class QueryLogResource
{

  private static final Logger log = new Logger(QueryLogResource.class);
  private final ObjectMapper jsonMapper;
  private final SqlLifecycleManager sqlLifecycleManager;
  private String requestLoggerBaseDir;
  private final static DateTimeFormatter DATE_TIME_FORMAT = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
  private final static DateTimeZone dateTimeZone = DateTimeZone.forTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));

  @Inject
  public QueryLogResource(
      @Json ObjectMapper jsonMapper,
      SqlLifecycleManager sqlLifecycleManager,
      RequestLogger requestLogger
  )
  {
    this.jsonMapper = jsonMapper;
    this.sqlLifecycleManager = sqlLifecycleManager;
    this.requestLoggerBaseDir = requestLogger.getLoggerProperties().getProperty("baseDir");
  }

  @GET
  @Path("/getAllQueryIds")
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(RulesResourceFilter.class)
  public Response getAllQueryIds() throws JsonProcessingException
  {
    Map<String, Long> queryIds = sqlLifecycleManager.getAllQueryIds();
    queryIds = sortMapByValues(queryIds);
    Map<String, String> queryTimes = new HashMap<>();

    for (Map.Entry<String, Long> e : queryIds.entrySet()) {
      DateTime t = new DateTime(e.getValue());
      queryTimes.put(e.getKey(), DATE_TIME_FORMAT.print(t.withZone(dateTimeZone)));
    }

    return Response.status(200)
                   .type(MediaType.APPLICATION_JSON_TYPE)
                   .entity(
                       jsonMapper.writeValueAsBytes(
                           queryTimes
                       )
                   )
                   .build();
  }

  public static <K extends Comparable, V extends Comparable> Map<K, V> sortMapByValues(Map<K, V> map)
  {
    HashMap<K, V> finalMap = new LinkedHashMap<K, V>();
    List<Map.Entry<K, V>> list = map.entrySet()
                                    .stream()
                                    .sorted((p2, p1) -> p1.getValue().compareTo(p2.getValue()))
                                    .collect(Collectors.toList());
    list.forEach(ele -> finalMap.put(ele.getKey(), ele.getValue()));
    return finalMap;
  }

  @GET
  @Path("/getSql/{id}")
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(RulesResourceFilter.class)
  public Response getSqlById(@PathParam("id") String sqlQueryId) throws JsonProcessingException
  {
    Pattern p = Pattern.compile("^\\d{4}-\\d{2}-\\d{2}.log");
    File[] files = new File(requestLoggerBaseDir).listFiles(new FilenameFilter()
    {
      @Override
      public boolean accept(File dir, String name)
      {
        Matcher m = p.matcher(name);
        return m.matches();
      }
    });

    List<String> sqlInfos = new ArrayList<>();
    for (File file : files) {
      try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file)))) {
        String tmp;
        while ((tmp = br.readLine()) != null) {
          if (tmp.contains(sqlQueryId)) {
            sqlInfos.add(tmp);
            if (sqlInfos.size() >= 2) {
              break;
            }
          }
        }
      }
      catch (Exception e) {
      }
      if (sqlInfos.size() >= 2) {
        break;
      }
    }

    return Response.status(200)
                   .type(MediaType.APPLICATION_JSON_TYPE)
                   .entity(
                       jsonMapper.writeValueAsBytes(
                           sqlInfos
                       )
                   )
                   .build();
  }

  @GET
  @Path("/getTopNQuery/{topN}")
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(RulesResourceFilter.class)
  public Response getTopNQuery(@PathParam("topN") int topN, @QueryParam("keyword") final String keyword)
      throws JsonProcessingException
  {
    Pattern p = Pattern.compile("^\\d{4}-\\d{2}-\\d{2}.log");
    File[] files = new File(requestLoggerBaseDir).listFiles(new FilenameFilter()
    {
      @Override
      public boolean accept(File dir, String name)
      {
        Matcher m = p.matcher(name);
        return m.matches();
      }
    });

    MinMaxPriorityQueue<String> maxHeap = MinMaxPriorityQueue
        .orderedBy(Ordering.from(new Comparator<String>()
        {
          String[] timeKeys = {"sqlQuery/time", "query/time"};

          @Override
          public int compare(String line1, String line2)
          {
            Long cost1 = costExtracted(line1);
            Long cost2 = costExtracted(line2);
            return cost2.compareTo(cost1);
          }

          private long costExtracted(String line1)
          {
            long cost1 = 0;

            String[] logArr = line1.split("\t");
            for (int i = 2; i < logArr.length; i++) {
              try {
                JsonNode node = jsonMapper.readTree(logArr[i]);
                for (String k : timeKeys) {
                  if (node.has(k)) {
                    cost1 = node.get(k).asLong();
                    break;
                  }
                }
              }
              catch (JsonProcessingException e) {
                log.error("fail to parse request log:[%s].", e.getMessage());
              }
              if (cost1 > 0) {
                break;
              }
            }
            return cost1;
          }
        }))
        .maximumSize(topN)
        .create();

    for (File file : files) {
      try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file)))) {
        String tmp;
        while ((tmp = br.readLine()) != null) {
          if (StringUtils.isNotEmpty(keyword) && !tmp.contains(keyword)) {
            continue;
          }
          maxHeap.add(tmp);
        }
      }
      catch (Exception e) {
        log.error("fail to read request log:[%s].", e.getMessage());
      }
    }

    return Response.status(200)
                   .type(MediaType.APPLICATION_JSON_TYPE)
                   .entity(
                       jsonMapper.writeValueAsBytes(
                           Lists.newArrayList(maxHeap.iterator())
                       )
                   )
                   .build();
  }
}
