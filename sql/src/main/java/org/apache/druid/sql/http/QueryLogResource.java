//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package org.apache.druid.sql.http;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.Ordering;
import com.google.inject.Inject;
import com.sun.jersey.spi.container.ResourceFilters;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.commons.lang3.StringUtils;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.http.security.RulesResourceFilter;
import org.apache.druid.server.log.RequestLogger;
import org.apache.druid.sql.SqlLifecycleManager;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

@Path("/druid/v2/query/")
public class QueryLogResource
{
  private static final Logger log = new Logger(QueryLogResource.class);
  private final ObjectMapper jsonMapper;
  private final SqlLifecycleManager sqlLifecycleManager;
  private String requestLoggerBaseDir;
  private static final DateTimeFormatter DATE_TIME_FORMAT = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
  private static final DateTimeZone dateTimeZone = DateTimeZone.forTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));

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
  @Consumes({"application/json"})
  @ResourceFilters({RulesResourceFilter.class})
  public Response getAllQueryIds() throws JsonProcessingException
  {
    Map<String, Long> queryIds = this.sqlLifecycleManager.getAllQueryIds();
    queryIds = sortMapByValues(queryIds);
    Map<String, String> queryTimes = new HashMap();
    Iterator var3 = queryIds.entrySet().iterator();

    while (var3.hasNext()) {
      Entry<String, Long> e = (Entry) var3.next();
      DateTime t = new DateTime(e.getValue());
      queryTimes.put(e.getKey(), DATE_TIME_FORMAT.print(t.withZone(dateTimeZone)));
    }

    return Response.status(200)
                   .type(MediaType.APPLICATION_JSON_TYPE)
                   .entity(this.jsonMapper.writeValueAsBytes(queryTimes))
                   .build();
  }

  public static <K extends Comparable, V extends Comparable> Map<K, V> sortMapByValues(Map<K, V> map)
  {
    HashMap<K, V> finalMap = new LinkedHashMap();
    List<Entry<K, V>> list = (List) map.entrySet().stream().sorted((p2, p1) -> {
      return ((Comparable) p1.getValue()).compareTo(p2.getValue());
    }).collect(Collectors.toList());
    list.forEach((ele) -> {
      Comparable var10000 = (Comparable) finalMap.put(ele.getKey(), ele.getValue());
    });
    return finalMap;
  }

  @GET
  @Path("/getSql/{id}")
  @Consumes({"application/json"})
  @ResourceFilters({RulesResourceFilter.class})
  public Response getSqlById(@PathParam("id") String sqlQueryId) throws JsonProcessingException
  {
    final Pattern p = Pattern.compile("^\\d{4}-\\d{2}-\\d{2}.log");
    File[] files = (new File(this.requestLoggerBaseDir)).listFiles(new FilenameFilter()
    {
      @Override
      public boolean accept(File dir, String name)
      {
        Matcher m = p.matcher(name);
        return m.matches();
      }
    });
    List<String> sqlInfos = new ArrayList();
    File[] var5 = files;
    int var6 = files.length;

    for (int var7 = 0; var7 < var6; ++var7) {
      File file = var5[var7];

      try {
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
        Throwable var10 = null;

        try {
          String tmp;
          try {
            while ((tmp = br.readLine()) != null) {
              if (tmp.contains(sqlQueryId)) {
                sqlInfos.add(tmp);
                if (sqlInfos.size() >= 2) {
                  break;
                }
              }
            }
          }
          catch (Throwable var20) {
            var10 = var20;
            throw var20;
          }
        }
        finally {
          if (br != null) {
            if (var10 != null) {
              try {
                br.close();
              }
              catch (Throwable var19) {
                var10.addSuppressed(var19);
              }
            } else {
              br.close();
            }
          }

        }
      }
      catch (Exception var22) {
      }

      if (sqlInfos.size() >= 2) {
        break;
      }
    }

    return Response.status(200)
                   .type(MediaType.APPLICATION_JSON_TYPE)
                   .entity(this.jsonMapper.writeValueAsBytes(sqlInfos))
                   .build();
  }

  @GET
  @Path("/getTopNQuery")
  @Consumes({"application/json"})
  @ResourceFilters({RulesResourceFilter.class})
  public Response getTopNQuery(
      @QueryParam("topN") int topN,
      @QueryParam("keyword") String keyword,
      @QueryParam("start") String start,
      @QueryParam("end") String end
  ) throws JsonProcessingException, ParseException
  {
    final Pattern p = Pattern.compile("^\\d{4}-\\d{2}-\\d{2}.log");
    File[] files = (new File(this.requestLoggerBaseDir)).listFiles(new FilenameFilter()
    {
      @Override
      public boolean accept(File dir, String name)
      {
        Matcher m = p.matcher(name);
        return m.matches();
      }
    });
    long startTime = getDateTime(start);
    long endTime = getDateTime(end);
    MinMaxPriorityQueue<String> maxHeap = getQueue(topN, this.jsonMapper);
    File[] var12 = files;
    int var13 = files.length;

label160:
    for (int var14 = 0; var14 < var13; ++var14) {
      File file = var12[var14];

      try {
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
        Throwable var17 = null;

        try {
          while (true) {
            String tmp;
            long time;
            do {
              do {
                do {
                  if ((tmp = br.readLine()) == null) {
                    continue label160;
                  }

                  time = getDateTime(tmp.split("\t")[0]);
                } while (time < startTime);
              } while (time > endTime);
            } while (StringUtils.isNotEmpty(keyword) && !tmp.contains(keyword));

            maxHeap.offer(tmp);
          }
        }
        catch (Throwable var29) {
          var17 = var29;
          throw var29;
        }
        finally {
          if (br != null) {
            if (var17 != null) {
              try {
                br.close();
              }
              catch (Throwable var28) {
                var17.addSuppressed(var28);
              }
            } else {
              br.close();
            }
          }

        }
      }
      catch (Exception var31) {
        log.error("fail to read request log:[%s].", new Object[]{var31.getMessage()});
      }
    }

    ArrayList result = new ArrayList();

    while (!maxHeap.isEmpty()) {
      result.add(maxHeap.poll());
    }

    return Response.status(200)
                   .type(MediaType.APPLICATION_JSON_TYPE)
                   .entity(this.jsonMapper.writeValueAsBytes(result))
                   .build();
  }

  @Nonnull
  public static MinMaxPriorityQueue<String> getQueue(int topN, final ObjectMapper jsonMapper)
  {
    MinMaxPriorityQueue<String> maxHeap = MinMaxPriorityQueue.orderedBy(Ordering.from(new Comparator<String>()
    {
      String[] timeKeys = new String[]{"sqlQuery/time", "query/time"};
      Ordering comparable = Comparators.naturalNullsFirst().reverse();

      @Override
      public int compare(String line1, String line2)
      {
        Long cost1 = this.costExtracted(line1);
        Long cost2 = this.costExtracted(line2);
        int compare = this.comparable.compare(cost1, cost2);
        return compare != 0 ? compare : 0;
      }

      private long costExtracted(String line)
      {
        long cost = 0L;
        String[] logArr = line.split("\t");

        for (int i = 2; i < logArr.length; ++i) {
          try {
            JsonNode node = jsonMapper.readTree(logArr[i]);
            String[] var7 = this.timeKeys;
            int var8 = var7.length;

            for (String k : var7) {
              if (node.has(k)) {
                cost = node.get(k).asLong();
                break;
              }
            }
          }
          catch (JsonProcessingException var11) {
            QueryLogResource.log.error("fail to parse request log:[%s].", new Object[]{var11.getMessage()});
          }

          if (cost > 0L) {
            break;
          }
        }

        return cost;
      }
    })).maximumSize(topN).create();
    return maxHeap;
  }

  public static long getDateTime(String dateTime) throws ParseException
  {
    String pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
    SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
    simpleDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    return simpleDateFormat.parse(dateTime).getTime();
  }
}
