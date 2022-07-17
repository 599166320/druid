package org.apache.druid.sql.http;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.sun.jersey.spi.container.ResourceFilters;
import io.vavr.Tuple3;
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
import org.apache.druid.promql.logical.*;
import org.apache.druid.query.*;
import org.apache.druid.server.http.security.DatasourceResourceFilter;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizationUtils;
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
import java.io.IOException;
import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
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
            @QueryParam("timeout") Integer timeout,
            @QueryParam("stats") String stats
    )
    {
        try {
            PromQLLexer lexer = new PromQLLexer(new ANTLRInputStream(query));
            CommonTokenStream tokenStream = new CommonTokenStream(lexer);
            PromQLParser parser = new PromQLParser(tokenStream);
            PromQLParser.ExpressionContext expressionContext = parser.expression();
            RangeVisitor rangeVisitor = new RangeVisitor();
            int range = rangeVisitor.visitExpression(expressionContext);
            int end = new BigDecimal(time).intValue();
            int start  = end-range;
            return queryRange(req,query,start+"",end+"",15,timeout);
        }catch (Exception e){
            log.error("Fail to query:"+e.getMessage());
            return Response.ok("{\"status\":\"error\",\"data\":{\"resultType\":\"scalar\",\"result\":[1657333806.183,\"2\"]}}").build();
        }
        //"resultType": "matrix" | "vector" | "scalar" | "string",
        //return Response.ok("{\"status\":\"success\",\"data\":{\"resultType\":\"scalar\",\"result\":[1657333806.183,\"2\"]}}").build();
    }


    @POST
    @Path("/query")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @ResourceFilters(DatasourceResourceFilter.class)
    public Response postQuery(
            @Context final HttpServletRequest req,
            @FormParam("query") String query,
            @FormParam("time") String time,
            @FormParam("timeout") Integer timeout,
            @FormParam("stats") String stats
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
            @QueryParam("end") String timeout
    )
    {
        return Response.ok("{\"status\":\"success\",\"data\":[]}").build();
    }

    @POST
    @Path("/query_exemplars")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @ResourceFilters(DatasourceResourceFilter.class)
    public Response postQueryExemplars(
            @Context final HttpServletRequest req,
            @FormParam("query") String query,
            @FormParam("start") String start,
            @FormParam("end") String end
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
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @ResourceFilters(DatasourceResourceFilter.class)
    public Response postRules(@Context final HttpServletRequest req)
    {
        return rules(req);
    }


    @GET
    @Path("/series")
    @Produces(MediaType.APPLICATION_JSON)
    @ResourceFilters(DatasourceResourceFilter.class)
    public Response series(
            @Context final HttpServletRequest req,
            @QueryParam("start") String start,
            @QueryParam("end") String end
            ) throws JsonProcessingException {

        Map<String,Object> responseMap = new HashMap<>();
        List<Map<String,Object>> dataList = new ArrayList<>();
        responseMap.put("data",dataList);
        responseMap.put("status","success");
        String[] matchs = req.getParameterValues("match[]");
        if(Objects.nonNull(matchs)){
            for(String promql : matchs){
                Response response = metadata(promql,start,end,req,String.join(",",PromQLTemrminalVisitor.SERIES_FIELDS));
                if(response.getStatus() == 200){
                    List<Map<String,Object>> datas = (List<Map<String, Object>>) response.getEntity();
                    for(Map<String,Object> data : datas){
                        Map<String,Object> series = new HashMap<>();
                        for(Map.Entry<String,Object> entry : data.entrySet()){
                            if("labels".equals(entry.getKey())){
                                List<String> lvs;
                                if(entry.getValue().toString().contains("[") && entry.getValue().toString().contains("]")){
                                    lvs = this.jsonMapper.readValue(entry.getValue().toString(), List.class);
                                }else{
                                    lvs = new ArrayList<>();
                                    lvs.add(entry.getValue().toString());
                                }
                                for(String lvStr : lvs){
                                    String [] lv = lvStr.split("=");
                                    series.put(lv[0], lv[1]);
                                }

                            }else if("name".equals(entry.getKey())){
                                series.put("__name__",String.valueOf(entry.getValue()));
                            }else {
                                series.put(entry.getKey(),String.valueOf(entry.getValue()));
                            }
                        }
                        dataList.add(series);
                    }
                }
            }
        }

        return Response.ok(responseMap).build();
        //return Response.ok("{\"status\":\"success\",\"data\":{\"groups\":[]}}").build();
        //return "{\"status\":\"error\",\"data\":{}}";
        //{"status":"success","data":[{"__name__":"mysql_exporter_collector_duration_seconds","collector":"collect.global_status","instance":"localhost:9104","job":"mysql"},{"__name__":"mysql_exporter_collector_duration_seconds","collector":"collect.global_variables","instance":"localhost:9104","job":"mysql"},{"__name__":"mysql_exporter_collector_duration_seconds","collector":"collect.info_schema.innodb_cmp","instance":"localhost:9104","job":"mysql"},{"__name__":"mysql_exporter_collector_duration_seconds","collector":"collect.info_schema.innodb_cmpmem","instance":"localhost:9104","job":"mysql"},{"__name__":"mysql_exporter_collector_duration_seconds","collector":"collect.info_schema.query_response_time","instance":"localhost:9104","job":"mysql"},{"__name__":"mysql_exporter_collector_duration_seconds","collector":"collect.slave_status","instance":"localhost:9104","job":"mysql"},{"__name__":"mysql_exporter_collector_duration_seconds","collector":"connection","instance":"localhost:9104","job":"mysql"}]}
    }



    @GET
    @Path("/labels")
    @Produces(MediaType.APPLICATION_JSON)
    @ResourceFilters(DatasourceResourceFilter.class)
    public Response labels(@Context final HttpServletRequest req,
                           @QueryParam("start") String start,
                           @QueryParam("end") String end
                           ) throws JsonProcessingException {
        String[] matchs = req.getParameterValues("match[]");
        //match多个promql
        Map<String,Object>  responseMap = new HashMap<>();
        Set<String> dataList = new HashSet<>(PromQLTemrminalVisitor.PUBLIC_FIELDS);
        responseMap.put("status","success");
        responseMap.put("data",dataList);
        if(Objects.nonNull(matchs)){
            for(String promql : matchs){
                Response response = metadata(promql,start,end,req," distinct "+PromQLTemrminalVisitor.KEYS_FIELDS);
                if(response.getStatus() == 200){
                    List<Map<String,Object>> datas = (List<Map<String, Object>>) response.getEntity();
                    for(Map<String,Object> data : datas){
                        for(Map.Entry<String,Object> entry : data.entrySet()){
                            if(PromQLTemrminalVisitor.KEYS_FIELDS.equals(entry.getKey())){
                                if(entry.getValue().toString().contains("[") && entry.getValue().toString().contains("]")){
                                    dataList.addAll(this.jsonMapper.readValue(entry.getValue().toString(), List.class));
                                }else{
                                    dataList.add(entry.getValue().toString());
                                }
                            }
                        }
                    }
                }
            }
        }
        return Response.ok(responseMap).build();
        //return Response.ok("{\"status\":\"success\",\"data\":[\"__name__\",\"branch\",\"buffer_pool\",\"code\",\"collector\",\"command\",\"error\",\"goversion\",\"handler\",\"innodb_version\",\"instance\",\"instrumentation\",\"job\",\"le\",\"level\",\"operation\",\"page_size\",\"quantile\",\"revision\",\"state\",\"version\",\"version_comment\"]}").build();
    }


    @POST
    @Path("/labels")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @ResourceFilters(DatasourceResourceFilter.class)
    public Response postLabels(@Context final HttpServletRequest req,
                           @FormParam("start") String start,
                           @FormParam("end") String end
                               ) throws JsonProcessingException {
        return labels(req,start,end);
    }


    @GET
    @Path("/label/{name}/values")
    @Produces(MediaType.APPLICATION_JSON)
    @ResourceFilters(DatasourceResourceFilter.class)
    public Response values(@Context final HttpServletRequest req,
                               @PathParam("name") String name,
                               @QueryParam("start") String start,
                               @QueryParam("end") String end
                             ) throws JsonProcessingException {

        String[] matchs = req.getParameterValues("match[]");

        String where = "";
        if("__name__".equals(name)){
            name = "name";
        }else if(!PromQLTemrminalVisitor.PUBLIC_FIELDS.contains(name)){
            where  = "  MV_CONTAINS(keys,'"+name+"') ";
        }

        if(Objects.nonNull(matchs)){
            PromQLLexer lexer = new PromQLLexer(new ANTLRInputStream(matchs[0]));
            CommonTokenStream tokenStream = new CommonTokenStream(lexer);
            PromQLParser parser = new PromQLParser(tokenStream);
            PromQLParser.ExpressionContext expressionContext = parser.expression();
            PromQLTemrminalVisitor.LabelMatcherVisitor labelMatcherVisitor = new PromQLTemrminalVisitor.LabelMatcherVisitor();
            if(!org.apache.commons.lang3.StringUtils.isEmpty(where)){
                where += " and ";
            }
            where  += labelMatcherVisitor.visitExpression(expressionContext);
        }

        if(Objects.nonNull(start) && Objects.nonNull(end)){
            BigDecimal startTime = new BigDecimal(start).multiply(new BigDecimal(1000));
            BigDecimal endTime = new BigDecimal(end).multiply(new BigDecimal(1000));
            if(!org.apache.commons.lang3.StringUtils.isEmpty(where)){
                where += " and ";
            }
            where += "  __time>=MILLIS_TO_TIMESTAMP("+startTime+") and __time<=MILLIS_TO_TIMESTAMP("+endTime+") ";
        }

        String sql;

        if(where.length()>0){
            where = " where "+where;
        }

        if(PromQLTemrminalVisitor.PUBLIC_FIELDS.contains(name)){
            sql  ="select distinct \""+name+"\" from "+PromQLTemrminalVisitor.TABLE+" "+where;
        }else{
            sql = " SELECT distinct VIRTUAL_COLUMN(\"labels\",\"keys\",'"+name+"') FROM "+PromQLTemrminalVisitor.TABLE +where;
        }

        sql += " limit "+PromQLTemrminalVisitor.LIMIT;

        Map<String,Object>  responseMap = new HashMap<>();
        List<String> dataList = new ArrayList<>();
        responseMap.put("status","success");
        responseMap.put("data",dataList);

        Response response = sqlExec(sql,req);
        if(response.getStatus() == 200){
            List<Map<String,Object>> datas = (List<Map<String, Object>>) response.getEntity();
            for(Map<String,Object> data : datas){
                for(Map.Entry<String,Object> entry : data.entrySet()){
                    if(PromQLTemrminalVisitor.PUBLIC_FIELDS.contains(name)){
                        dataList.add(String.valueOf(entry.getValue()));
                    }else{
                        dataList.add(String.valueOf(entry.getValue()).split("=")[1]);
                    }
                }
            }
        }
        return Response.ok(responseMap).build();
        //return Response.ok("{\"status\":\"success\",\"data\":{}}").build();
    }

    @POST
    @Path("/label/{name}/values")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @ResourceFilters(DatasourceResourceFilter.class)
    public Response PostValues(
            @Context final HttpServletRequest req,
            @PathParam("name") String name,
            @FormParam("start") String start,
            @FormParam("end") String end
                                 ) throws JsonProcessingException {
        return values(req,name,start,end);
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
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @ResourceFilters(DatasourceResourceFilter.class)
    public Response postMetadata(@Context final HttpServletRequest req)
    {
        return metadata(req);
    }

    @GET
    @Path("/query_range")
    @Produces(MediaType.APPLICATION_JSON)
    @ResourceFilters(DatasourceResourceFilter.class)
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
        BigDecimal startTime = new BigDecimal(start).multiply(new BigDecimal(1000));
        BigDecimal endTime = new BigDecimal(end).multiply(new BigDecimal(1000));

        Operator operator = exec(query,step,startTime.longValue(),endTime.longValue(),req);
        if(operator instanceof SeriesSetOperator){
            data.put("resultType","matrix");
            List<Map<String,Object>> result = new ArrayList<>();
            data.put("result",result);
            List<Series> seriesSet = ((SeriesSetOperator)operator).getSeriesSet();
            for(Series ss:seriesSet){
                Map<String,Object> smap = new HashMap<>();
                smap.put("metric",ss.getLabels());
                TreeMap<Integer, Double> dp = ss.getDataPoint();
                List<Object[]> tvs = new ArrayList<>();
                smap.put("values",tvs);
                for(Map.Entry<Integer, Double> entry:dp.entrySet()){
                    int ts = entry.getKey()*1000;
                    if( startTime.intValue() <= ts && endTime.intValue() >= ts)
                    {
                        tvs.add(new Object[]{entry.getKey(),entry.getValue()+""});
                    }
                }
                result.add(smap);
            }
        }else if(operator instanceof NumberLiteralOperator){
            data.put("resultType","scalar");
            data.put("result", new Object[]{new BigDecimal(end).doubleValue(),((NumberLiteralOperator) operator).getNumber()+""});
        }
        return Response.ok(responseMap).build();
        //return Response.ok("{\"status\":\"success\",\"data\":{\"resultType\":\"matrix\",\"result\":[]}}").build();
    }


    @POST
    @Path("/query_range")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @ResourceFilters(DatasourceResourceFilter.class)
    public Response postQueryRange(
            @Context final HttpServletRequest req,
            @FormParam("query") String query,
            @FormParam("start") String start,
            @FormParam("end") String end,
            @FormParam("step") Integer step,
            @FormParam("timeout") Integer timeout
    ) throws IOException, ExecutionException, InterruptedException {
        return this.queryRange(req,query,start,end,step,timeout);
    }

    public Operator exec(String promql,int step,long start,long end,final HttpServletRequest req) throws IOException, ExecutionException, InterruptedException {
        PromQLLexer lexer = new PromQLLexer(new ANTLRInputStream(promql));
        CommonTokenStream tokenStream = new CommonTokenStream(lexer);
        PromQLParser parser = new PromQLParser(tokenStream);
        PromQLParser.ExpressionContext expressionContext = parser.expression();

        PromQLTemrminalVisitor promQLTemrminalVisitor = new PromQLTemrminalVisitor(start,end);
        Map<String,String> sqlMap =  promQLTemrminalVisitor.visitExpression(expressionContext);
        Map<String, Object> context = new HashMap<>();
        List<SqlParameter> parameters = new ArrayList<>();
        List<CompletableFuture<Tuple3<String, List<Map<String,Object>> ,Yielder<Object[]>>>> completableFutureList = new ArrayList<>();

        AuthenticationResult authResult = AuthorizationUtils.authenticationResultFromRequest(req);
        for(Map.Entry<String,String> entry:sqlMap.entrySet()){
            SqlQuery sqlQuery = new SqlQuery(entry.getValue(),ResultFormat.ARRAY,true,context,parameters);
            final SqlLifecycle lifecycle = sqlLifecycleFactory.factorize();
            final String sqlQueryId = lifecycle.initialize(sqlQuery.getQuery(), sqlQuery.getContext());
            lifecycle.setParameters(sqlQuery.getParameterList());
            //lifecycle.validateAndAuthorize(req);
            lifecycle.validateAndAuthorize(authResult);

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
                        return new Tuple3(entry.getKey(),toList(yielder0,fieldList,timeColumns,dateColumns,otherTypeColumns,timeZone),yielder0);
                    } catch (Exception e) {
                        log.error("fail to close yielder:"+e.getMessage());
                    }
                    return  new Tuple3(entry.getKey(),null,yielder0);
                }));

            } catch (Exception e) {
                log.error("fail to close yielder:"+e.getMessage());
                return null;
            }
            finally {
                Thread.currentThread().setName(currThreadName);
            }
        }
        Map<String, SeriesSetOperator> seriesSetOperatorMap = new ConcurrentHashMap<>();
        for(CompletableFuture<Tuple3<String, List<Map<String,Object>>, Yielder<Object[]>>> future:completableFutureList){
            Tuple3<String, List<Map<String,Object>>, Yielder<Object[]> > tuple3 = future.get();
            String k = tuple3._1;
            tuple3._3.close();
            List<Map<String,Object>> datas = tuple3._2;
            if(Objects.isNull(datas)){
                continue;
            }

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
            seriesSetOperatorMap.put(k,seriesSetOperator.vectorSelectorSingle(step));
        }
        PromQLVisitor visitor = new PromQLVisitor();
        visitor.setRawSeriesSetOperatorMap(seriesSetOperatorMap);
        visitor.setStep(step);
        Operator result = visitor.visitExpression(expressionContext);
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
                Object value = null;
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
                }else if("keys".equals(fieldList.get(i).getName())){
                    cols.put(fieldList.get(i).getName(),value.toString());
                }else if(!"keys".equals(fieldList.get(i).getName()) && !"__time".equals(fieldList.get(i).getName())){
                    cols.put(fieldList.get(i).getName(), String.valueOf(value));
                }
            }
            yielder = yielder.next(null);
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


    private Response sqlExec(String sql, final HttpServletRequest req) throws JsonProcessingException {

        Map<String, Object> context = new HashMap<>();
        List<SqlParameter> parameters = new ArrayList<>();
        final SqlQuery sqlQuery = new SqlQuery(sql,ResultFormat.ARRAY,true,context,parameters);
        final SqlLifecycle lifecycle = sqlLifecycleFactory.factorize();
        final String sqlQueryId = lifecycle.initialize(sqlQuery.getQuery(), sqlQuery.getContext());
        final String remoteAddr = req.getRemoteAddr();
        final String currThreadName = Thread.currentThread().getName();
        AuthenticationResult authResult = AuthorizationUtils.authenticationResultFromRequest(req);
        try {
            Thread.currentThread().setName(StringUtils.format("sql[%s]", sqlQueryId));

            lifecycle.setParameters(sqlQuery.getParameterList());
            lifecycle.validateAndAuthorize(authResult);
            final PlannerContext plannerContext = lifecycle.plan();
            final DateTimeZone timeZone = plannerContext.getTimeZone();

            // Remember which columns are time-typed, so we can emit ISO8601 instead of millis values.
            // Also store list of all column names, for X-Druid-Sql-Columns header.
            final List<RelDataTypeField> fieldList = lifecycle.rowType().getFieldList();
            final boolean[] timeColumns = new boolean[fieldList.size()];
            final boolean[] dateColumns = new boolean[fieldList.size()];
            final boolean[] otherTypeColumns = new boolean[fieldList.size()];

            for (int i = 0; i < fieldList.size(); i++) {
                final SqlTypeName sqlTypeName = fieldList.get(i).getType().getSqlTypeName();
                timeColumns[i] = sqlTypeName == SqlTypeName.TIMESTAMP;
                dateColumns[i] = sqlTypeName == SqlTypeName.DATE;
                otherTypeColumns[i] = sqlTypeName == SqlTypeName.OTHER;
            }

            final Yielder<Object[]> yielder0 = Yielders.each(lifecycle.execute());

            List<Map<String,Object>> datas = toList(yielder0,fieldList,timeColumns,dateColumns,otherTypeColumns,timeZone);

            try {
                return Response
                        .ok(datas)
                        .header("X-Druid-SQL-Query-Id", sqlQueryId)
                        .build();
            }
            catch (Throwable e) {
                // make sure to close yielder if anything happened before starting to serialize the response.
                yielder0.close();
                throw new RuntimeException(e);
            }
        }
        catch (QueryCapacityExceededException cap) {
            lifecycle.emitLogsAndMetrics(cap, remoteAddr, -1);
            return buildNonOkResponse(QueryCapacityExceededException.STATUS_CODE, cap);
        }
        catch (QueryUnsupportedException unsupported) {
            lifecycle.emitLogsAndMetrics(unsupported, remoteAddr, -1);
            return buildNonOkResponse(QueryUnsupportedException.STATUS_CODE, unsupported);
        }
        catch (QueryTimeoutException timeout) {
            lifecycle.emitLogsAndMetrics(timeout, remoteAddr, -1);
            return buildNonOkResponse(QueryTimeoutException.STATUS_CODE, timeout);
        }
        catch (SqlPlanningException | ResourceLimitExceededException e) {
            lifecycle.emitLogsAndMetrics(e, remoteAddr, -1);
            return buildNonOkResponse(BadQueryException.STATUS_CODE, e);
        }
        catch (ForbiddenException e) {
            throw e; // let ForbiddenExceptionMapper handle this
        }
        catch (Exception e) {
            log.warn(e, "Failed to handle query: %s", sqlQuery);
            lifecycle.emitLogsAndMetrics(e, remoteAddr, -1);

            final Exception exceptionToReport;

            if (e instanceof RelOptPlanner.CannotPlanException) {
                exceptionToReport = new ISE("Cannot build plan for query: %s", sqlQuery.getQuery());
            } else {
                exceptionToReport = e;
            }
            return buildNonOkResponse(
                    Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(),
                    QueryInterruptedException.wrapIfNeeded(exceptionToReport)
            );
        }
        finally {
            Thread.currentThread().setName(currThreadName);
        }
    }

    private Response metadata(String promql,String start,String end,final HttpServletRequest req,String fields) throws JsonProcessingException {

        PromQLLexer lexer = new PromQLLexer(new ANTLRInputStream(promql));
        CommonTokenStream tokenStream = new CommonTokenStream(lexer);
        PromQLParser parser = new PromQLParser(tokenStream);
        PromQLParser.ExpressionContext expressionContext = parser.expression();

        PromQLTemrminalVisitor.LabelMatcherVisitor labelMatcherVisitor = new PromQLTemrminalVisitor.LabelMatcherVisitor();
        String where  = labelMatcherVisitor.visitExpression(expressionContext);

        if(Objects.nonNull(start) && Objects.nonNull(end)){
            BigDecimal startTime = new BigDecimal(start).multiply(new BigDecimal(1000));
            BigDecimal endTime = new BigDecimal(end).multiply(new BigDecimal(1000));
            where += " and __time>=MILLIS_TO_TIMESTAMP("+startTime+") and __time<=MILLIS_TO_TIMESTAMP("+endTime+") ";
        }

        if(org.apache.commons.lang3.StringUtils.isNotEmpty(where)){
            where = " where "+where;
        }

        String sql = "select "+fields+" from "+PromQLTemrminalVisitor.TABLE+" "+where+"  limit "+PromQLTemrminalVisitor.LIMIT;

        return sqlExec(sql,req);
    }

    private Response metadata(String promql, final HttpServletRequest req,String fields) throws JsonProcessingException {
        return metadata(promql,null,null,req,fields);
    }

    Response buildNonOkResponse(int status, Exception e) throws JsonProcessingException
    {
        return Response.status(status)
                .type(MediaType.APPLICATION_JSON_TYPE)
                .entity(jsonMapper.writeValueAsBytes(e))
                .build();
    }
}
