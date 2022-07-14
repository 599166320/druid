package org.apache.druid.sql.http;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.CountingOutputStream;
import com.google.inject.Inject;
import com.sun.jersey.spi.container.ResourceFilters;
import io.vavr.Tuple2;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.promql.PromQLVisitor;
import org.apache.druid.promql.antlr.PromQLLexer;
import org.apache.druid.promql.antlr.PromQLParser;
import org.apache.druid.promql.data.Series;
import org.apache.druid.promql.logical.Operator;
import org.apache.druid.promql.logical.PromQLTemrminalVisitor;
import org.apache.druid.promql.logical.SeriesSetOperator;
import org.apache.druid.query.*;
import org.apache.druid.server.http.security.DatasourceResourceFilter;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.sql.SqlLifecycle;
import org.apache.druid.sql.SqlLifecycleFactory;
import org.apache.druid.sql.SqlPlanningException;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.tsg.TSG;
import org.joda.time.DateTimeZone;
import org.joda.time.format.ISODateTimeFormat;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Path("/api/v1/")
public class PromQLResource {

    private static final Logger log = new Logger(PromQLResource.class);
    private final ObjectMapper jsonMapper;
    private final SqlLifecycleFactory sqlLifecycleFactory;

    @Inject
    public PromQLResource(
            @Json ObjectMapper jsonMapper,
            SqlLifecycleFactory sqlLifecycleFactory
    )
    {
        this.jsonMapper = Preconditions.checkNotNull(jsonMapper, "jsonMapper");
        this.sqlLifecycleFactory = Preconditions.checkNotNull(sqlLifecycleFactory, "sqlLifecycleFactory");
    }

    @GET
    @Path("/status/buildinfo")
    @Produces(MediaType.APPLICATION_JSON)
    @ResourceFilters(DatasourceResourceFilter.class)
    public Response buildinfo()
    {
        return  Response.ok("{\"status\":\"success\",\"data\":{\"version\":\"2.29.0-rc.0\",\"revision\":\"d9c31f3e663a6aab9318e61680ca029eda2ea29c\",\"branch\":\"HEAD\",\"buildUser\":\"root@e1c53e13bfb0\",\"buildDate\":\"20210730-14:11:47\",\"goVersion\":\"go1.16.6\"}}").build();
    }

    @GET
    @Path("/query")
    @Produces(MediaType.APPLICATION_JSON)
    @ResourceFilters(DatasourceResourceFilter.class)
    public Response query(
            @Context final HttpServletRequest req,
            @QueryParam("query") String query,
            @QueryParam("time") String time,
            @QueryParam("timeout") Long timeout,
            @QueryParam("stats") String stats
    )
    {
        return Response.ok("{\"status\":\"success\",\"data\":{\"resultType\":\"scalar\",\"result\":[1657333806.183,\"2\"]}}").build();
    }


    @POST
    @Path("/query")
    @Produces(MediaType.APPLICATION_JSON)
    @ResourceFilters(DatasourceResourceFilter.class)
    public Response postQuery(
            @Context final HttpServletRequest req,
            @QueryParam("query") String query,
            @QueryParam("time") String time,
            @QueryParam("timeout") Long timeout,
            @QueryParam("stats") String stats
    )
    {
        return this.query(req,query,time,timeout,stats);
    }


    @GET
    @Path("/query_exemplars")
    @Produces(MediaType.APPLICATION_JSON)
    @ResourceFilters(DatasourceResourceFilter.class)
    public Response queryExemplars(
            @Context final HttpServletRequest req,
            @QueryParam("query") String query,
            @QueryParam("start") String time,
            @QueryParam("end") Long timeout
    )
    {
        return Response.ok("{\"status\":\"success\",\"data\":[]}").build();
    }

    @POST
    @Path("/query_exemplars")
    @Produces(MediaType.APPLICATION_JSON)
    @ResourceFilters(DatasourceResourceFilter.class)
    public Response postQueryExemplars(
            @Context final HttpServletRequest req,
            @QueryParam("query") String query,
            @QueryParam("start") String start,
            @QueryParam("end") Long end
    )
    {
        return this.queryExemplars(req,query,start,end);
    }

    @GET
    @Path("/rules")
    @Produces(MediaType.APPLICATION_JSON)
    @ResourceFilters(DatasourceResourceFilter.class)
    public Response rules(@Context final HttpServletRequest req)
    {
        return Response.ok("{\"status\":\"success\",\"data\":{\"groups\":[]}}").build();
    }

    @POST
    @Path("/rules")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @ResourceFilters(DatasourceResourceFilter.class)
    public Response postRules(@Context final HttpServletRequest req)
    {
        return rules(req);
    }


    @GET
    @Path("/labels")
    @Produces(MediaType.APPLICATION_JSON)
    @ResourceFilters(DatasourceResourceFilter.class)
    public Response labels(@Context final HttpServletRequest req,
                           @QueryParam("start") String start,
                           @QueryParam("end") Long end)
    {
        return Response.ok("{\"status\":\"success\",\"data\":[\"__name__\",\"branch\",\"buffer_pool\",\"code\",\"collector\",\"command\",\"error\",\"goversion\",\"handler\",\"innodb_version\",\"instance\",\"instrumentation\",\"job\",\"le\",\"level\",\"operation\",\"page_size\",\"quantile\",\"revision\",\"state\",\"version\",\"version_comment\"]}").build();
    }


    @POST
    @Path("/labels")
    @Produces(MediaType.APPLICATION_JSON)
    @ResourceFilters(DatasourceResourceFilter.class)
    public Response postLabels(@Context final HttpServletRequest req,
                           @QueryParam("start") String start,
                           @QueryParam("end") Long end)
    {
        return labels(req,start,end);
    }


    @GET
    @Path("/label/__name__/values")
    @Produces(MediaType.APPLICATION_JSON)
    @ResourceFilters(DatasourceResourceFilter.class)
    public Response labelNames(@Context final HttpServletRequest req,
                               @QueryParam("start") String start,
                               @QueryParam("end") Long end)
    {
        return Response.ok("{\"status\":\"success\",\"data\":{}}").build();
    }

    @POST
    @Path("/label/__name__/values")
    @Produces(MediaType.APPLICATION_JSON)
    @ResourceFilters(DatasourceResourceFilter.class)
    public Response postLabelNames(@Context final HttpServletRequest req,
                               @QueryParam("start") String start,
                               @QueryParam("end") Long end)
    {
        return labelNames(req,start,end);
    }

    @GET
    @Path("/metadata")
    @Produces(MediaType.APPLICATION_JSON)
    @ResourceFilters(DatasourceResourceFilter.class)
    public Response metadata(@Context final HttpServletRequest req)
    {
        return Response.ok("{\"status\":\"success\",\"data\":{}}").build();
    }

    @POST
    @Path("/metadata")
    @Produces(MediaType.APPLICATION_JSON)
    @ResourceFilters(DatasourceResourceFilter.class)
    public Response postMetadata(@Context final HttpServletRequest req)
    {
        return metadata(req);
    }

    @GET
    @Path("/query_range")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    //@ResourceFilters(DatasourceResourceFilter.class)
    public Response queryRange(
            @Context final HttpServletRequest req,
            @QueryParam("query") String query,
            @QueryParam("start") String start,
            @QueryParam("end") String end,
            @QueryParam("step") Integer step,
            @QueryParam("timeout") Integer timeout
    ) throws IOException, ExecutionException, InterruptedException {
        Map<String,Object> responseMap = new HashMap<>();
        Map<String,Object> data = new HashMap<>();
        responseMap.put("status","success");
        responseMap.put("data",data);
        data.put("resultType","matrix");
        List<Map<String,Object>> result = new ArrayList<>();
        data.put("result",result);
        BigDecimal startTime = new BigDecimal(start).multiply(new BigDecimal(1000));
        BigDecimal endTime = new BigDecimal(end).multiply(new BigDecimal(1000));

        SeriesSetOperator seriesSetOperator = exec(query,step,startTime.longValue()-startTime.longValue()%60_000,endTime.longValue()-endTime.longValue()%60_000,req);
        List<Series> seriesSet = seriesSetOperator.getSeriesSet();
        for(Series ss:seriesSet){
            Map<String,Object> smap = new HashMap<>();
            smap.put("metric",ss.getLabels());
            TreeMap<Integer, Double> dp = ss.getDataPoint();
            List<Object[]> tvs = new ArrayList<>();
            smap.put("values",tvs);
            for(Map.Entry<Integer, Double> entry:dp.entrySet()){
                tvs.add(new Object[]{entry.getKey(),entry.getValue()+""});
            }
            result.add(smap);
        }
        return Response.ok(responseMap).build();
        //return Response.ok("{\"status\":\"success\",\"data\":{\"resultType\":\"matrix\",\"result\":[]}}").build();
    }


    @POST
    @Path("/query_range")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    //@ResourceFilters(DatasourceResourceFilter.class)
    public Response postQueryRange(
            @Context final HttpServletRequest req,
            @QueryParam("query") String query,
            @QueryParam("start") String start,
            @QueryParam("end") String end,
            @QueryParam("step") Integer step,
            @QueryParam("timeout") Integer timeout
    ) throws IOException, ExecutionException, InterruptedException {
        return this.queryRange(req,query,start,end,step,timeout);
    }

    public SeriesSetOperator exec(String promql,int step,long start,long end,final HttpServletRequest req) throws IOException, ExecutionException, InterruptedException {
        PromQLLexer lexer = new PromQLLexer(new ANTLRInputStream(promql));
        CommonTokenStream tokenStream = new CommonTokenStream(lexer);
        PromQLParser parser = new PromQLParser(tokenStream);
        PromQLParser.ExpressionContext expressionContext = parser.expression();

        PromQLTemrminalVisitor promQLTemrminalVisitor = new PromQLTemrminalVisitor(start,end, ImmutableSet.of("name","env","app","cluster","le","ip","bu","instance","p","port","quantile"));
        Map<String,String> sqlMap =  promQLTemrminalVisitor.visitExpression(expressionContext);
        Map<String, Object> context = new HashMap<>();
        List<SqlParameter> parameters = new ArrayList<>();
        List<CompletableFuture<Tuple2<String, List<Map<String,Object>> >>> completableFutureList = new ArrayList<>();

        for(Map.Entry<String,String> entry:sqlMap.entrySet()){
            SqlQuery sqlQuery = new SqlQuery(entry.getValue(),ResultFormat.ARRAY,true,context,parameters);
            final SqlLifecycle lifecycle = sqlLifecycleFactory.factorize();
            final String sqlQueryId = lifecycle.initialize(sqlQuery.getQuery(), sqlQuery.getContext());
            lifecycle.setParameters(sqlQuery.getParameterList());
            lifecycle.validateAndAuthorize(req);

            final String currThreadName = Thread.currentThread().getName();
            try {
                Thread.currentThread().setName(StringUtils.format("sql[%s]", sqlQueryId));
                final PlannerContext plannerContext = lifecycle.plan();
                final DateTimeZone timeZone = plannerContext.getTimeZone();

                // Remember which columns are time-typed, so we can emit ISO8601 instead of millis values.
                // Also store list of all column names, for X-Druid-Sql-Columns header.
                final List<RelDataTypeField> fieldList = lifecycle.rowType().getFieldList();
                final boolean[] timeColumns = new boolean[fieldList.size()];
                final boolean[] dateColumns = new boolean[fieldList.size()];
                final boolean[] otherTypeColumns = new boolean[fieldList.size()];
                //final String[] columnNames = new String[fieldList.size()];

                for (int i = 0; i < fieldList.size(); i++) {
                    final SqlTypeName sqlTypeName = fieldList.get(i).getType().getSqlTypeName();
                    timeColumns[i] = sqlTypeName == SqlTypeName.TIMESTAMP;
                    dateColumns[i] = sqlTypeName == SqlTypeName.DATE;
                    otherTypeColumns[i] = sqlTypeName == SqlTypeName.OTHER;
                    //columnNames[i] = fieldList.get(i).getName();
                }
                final Yielder<Object[]> yielder0 = Yielders.each(lifecycle.execute());
                completableFutureList.add(CompletableFuture.supplyAsync(()-> {
                    try {
                        return new Tuple2(entry.getKey(),toList(yielder0,fieldList,timeColumns,dateColumns,otherTypeColumns,timeZone));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    return null;
                }));

            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
            finally {
                Thread.currentThread().setName(currThreadName);
            }
        }
        Map<String, SeriesSetOperator> seriesSetOperatorMap = new ConcurrentHashMap<>();
        for(CompletableFuture<Tuple2<String, List<Map<String,Object>> >> future:completableFutureList){
            Tuple2<String, List<Map<String,Object>> > tuple2 = future.get();
            String k = tuple2._1;
            List<Map<String,Object>> datas = tuple2._2;

            Map<String,Series> seriesMap = new HashMap<>();
            for(Map<String,Object> row:datas){
                Long t = (Long) row.get("t");
                Double v = (Double) row.get("v");
                row.remove("t");
                row.remove("v");

                Series series;
                TreeMap<String, Object> labels = new TreeMap<>(row);
                StringBuilder kBuilder = new StringBuilder();
                for(Map.Entry<String, Object> e:labels.entrySet()){
                    kBuilder.append(e.getKey()).append(":").append(e.getValue()).append(",");
                }

                if(seriesMap.containsKey(kBuilder.toString())){
                    series = seriesMap.get(kBuilder.toString());
                    series.getDataPoint().put(t.intValue(),v);
                }else {
                    series = new Series();
                    TreeMap<Integer,Double> dataPoint = new TreeMap<>();
                    dataPoint.put(t.intValue(),v);
                    series.setDataPoint(dataPoint);
                    series.setLabels(labels);
                    seriesMap.put(kBuilder.toString(),series);
                }
            }
            SeriesSetOperator seriesSetOperator = new SeriesSetOperator();
            List<Series> seriesSet = new ArrayList<>(seriesMap.values());
            seriesSetOperator.setSeriesSet(seriesSet);
            seriesSetOperatorMap.put(k,seriesSetOperator);
        }
        PromQLVisitor visitor = new PromQLVisitor();
        visitor.setRawSeriesSetOperatorMap(seriesSetOperatorMap);
        visitor.setStep(step);
        SeriesSetOperator result = (SeriesSetOperator) visitor.visitExpression(expressionContext);
        return result;
    }


    List<Map<String,Object>> toList(Yielder<Object[]> yielder,
                                    final List<RelDataTypeField> fieldList,
                                    final boolean[] timeColumns,
                                    final boolean[] dateColumns,
                                    boolean[] otherTypeColumns,
                                    final DateTimeZone timeZone) throws JsonProcessingException {
        List<Map<String,Object>> rawRows = new LinkedList<>();
        Set<String> tsgSet = new HashSet<>();
        while (!yielder.isDone()) {
            final Object[] row = yielder.get();
            Map<String,Object> cols = new LinkedHashMap<>();
            rawRows.add(cols);
            for (int i = 0; i < fieldList.size(); i++) {
                Object value;
                if (row[i] == null) {
                    value = null;
                } else if (timeColumns[i]) {
                    value = ISODateTimeFormat.dateTime().print(
                            Calcites.calciteTimestampToJoda((long) row[i], timeZone)
                    );
                } else if (dateColumns[i]) {
                    value = ISODateTimeFormat.dateTime().print(
                            Calcites.calciteDateToJoda((int) row[i], timeZone)
                    );
                }else if (otherTypeColumns[i]) {
                    try {
                        String base64;
                        if(row[i] instanceof  char[]){
                            base64 = new String((char[])row[i]);
                        }else {
                            base64 = row[i].toString().replaceAll("\"","");
                        }
                        value = TSG.getTimeAndValues(base64);
                        tsgSet.add(fieldList.get(i).getName());
                    }catch (Exception e){
                        value = row[i];
                        log.error("fail to parse tsg..."+e.getMessage());
                    }
                } else {
                    value = row[i];
                }

                if(value instanceof String && org.apache.commons.lang3.StringUtils.isEmpty(value.toString())){
                    continue;
                }
                if(value instanceof TreeMap){
                    cols.put(fieldList.get(i).getName(), value);
                }else if("name".equals(fieldList.get(i).getName())){
                    cols.put("__name__", value);
                }else if("labels".equals(fieldList.get(i).getName())){
                    List<String> lvs;
                    if(value.toString().contains("[") && value.toString().contains("]")){
                        lvs = this.jsonMapper.readValue(value.toString(), List.class);
                    }else{
                        lvs = new ArrayList<>();
                        lvs.add(value.toString());
                    }
                    for(String lvStr : lvs){
                        String [] lv = lvStr.split("=");
                        cols.put(lv[0], lv[1]);
                    }
                }else if(!"keys".equals(fieldList.get(i).getName()) && !"__time".equals(fieldList.get(i).getName())){
                    cols.put(fieldList.get(i).getName(), String.valueOf(value));
                }
            }
            yielder = yielder.next(null);
        }

        try {
            yielder.close();
        }catch (Exception e){
            log.error("fail to close yielder"+e.getMessage());
        }

        if(tsgSet.size() == 0){
            return rawRows;
        }
        List<Map<String,Object>> newRows = new LinkedList<>();
        for(Map<String,Object> rawRow:rawRows){
            TreeMap<Long,Double> treeMap = null;
            for(String timeAndValueField:tsgSet){
                if(rawRow.containsKey(timeAndValueField)){
                    treeMap = (TreeMap<Long, Double>) rawRow.get(timeAndValueField);
                    rawRow.remove(timeAndValueField);
                }else{
                    newRows.add(rawRow);
                }
            }
            for(Map.Entry<Long, Double> e:treeMap.entrySet()){
                Map<String, Object> newRow = new LinkedHashMap<>(rawRow);
                newRow.put("t",e.getKey());
                newRow.put("v",e.getValue());
                newRows.add(newRow);
            }
        }
        return newRows;
    }

    Response buildNonOkResponse(int status, Exception e) throws JsonProcessingException
    {
        return Response.status(status)
                .type(MediaType.APPLICATION_JSON_TYPE)
                .entity(jsonMapper.writeValueAsBytes(e))
                .build();
    }
}
