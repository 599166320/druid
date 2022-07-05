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

package org.apache.druid.sql.http;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.io.CountingOutputStream;
import com.google.inject.Inject;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.BadQueryException;
import org.apache.druid.query.QueryCapacityExceededException;
import org.apache.druid.query.QueryInterruptedException;
import org.apache.druid.query.QueryTimeoutException;
import org.apache.druid.query.QueryUnsupportedException;
import org.apache.druid.query.ResourceLimitExceededException;
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
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.StreamingOutput;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.*;

@Path("/druid/v2/sql/")
public class SqlResource
{
  private static final Logger log = new Logger(SqlResource.class);

  private final ObjectMapper jsonMapper;
  private final SqlLifecycleFactory sqlLifecycleFactory;

  @Inject
  public SqlResource(
      @Json ObjectMapper jsonMapper,
      SqlLifecycleFactory sqlLifecycleFactory
  )
  {
    this.jsonMapper = Preconditions.checkNotNull(jsonMapper, "jsonMapper");
    this.sqlLifecycleFactory = Preconditions.checkNotNull(sqlLifecycleFactory, "sqlLifecycleFactory");
  }


  public Response doPost1(
      final SqlQuery sqlQuery,
      @Context final HttpServletRequest req
  ) throws IOException
  {
    final SqlLifecycle lifecycle = sqlLifecycleFactory.factorize();
    final String sqlQueryId = lifecycle.initialize(sqlQuery.getQuery(), sqlQuery.getContext());
    final String remoteAddr = req.getRemoteAddr();
    final String currThreadName = Thread.currentThread().getName();

    try {
      Thread.currentThread().setName(StringUtils.format("sql[%s]", sqlQueryId));

      lifecycle.setParameters(sqlQuery.getParameterList());
      lifecycle.validateAndAuthorize(req);
      final PlannerContext plannerContext = lifecycle.plan();
      final DateTimeZone timeZone = plannerContext.getTimeZone();

      // Remember which columns are time-typed, so we can emit ISO8601 instead of millis values.
      // Also store list of all column names, for X-Druid-Sql-Columns header.
      final List<RelDataTypeField> fieldList = lifecycle.rowType().getFieldList();
      final boolean[] timeColumns = new boolean[fieldList.size()];
      final boolean[] dateColumns = new boolean[fieldList.size()];
      final String[] columnNames = new String[fieldList.size()];

      for (int i = 0; i < fieldList.size(); i++) {
        final SqlTypeName sqlTypeName = fieldList.get(i).getType().getSqlTypeName();
        timeColumns[i] = sqlTypeName == SqlTypeName.TIMESTAMP;
        dateColumns[i] = sqlTypeName == SqlTypeName.DATE;
        columnNames[i] = fieldList.get(i).getName();
      }

      final Yielder<Object[]> yielder0 = Yielders.each(lifecycle.execute());

      try {
        return Response
            .ok(
                (StreamingOutput) outputStream -> {
                  Exception e = null;
                  CountingOutputStream os = new CountingOutputStream(outputStream);
                  Yielder<Object[]> yielder = yielder0;

                  try (final ResultFormat.Writer writer = sqlQuery.getResultFormat()
                                                                  .createFormatter(os, jsonMapper)) {
                    writer.writeResponseStart();

                    if (sqlQuery.includeHeader()) {
                      writer.writeHeader(Arrays.asList(columnNames));
                    }

                    while (!yielder.isDone()) {
                      final Object[] row = yielder.get();
                      writer.writeRowStart();
                      for (int i = 0; i < fieldList.size(); i++) {
                        final Object value;

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
                        } else {
                          value = row[i];
                        }

                        writer.writeRowField(fieldList.get(i).getName(), value);
                      }
                      writer.writeRowEnd();
                      yielder = yielder.next(null);
                    }

                    writer.writeResponseEnd();
                  }
                  catch (Exception ex) {
                    e = ex;
                    log.error(ex, "Unable to send SQL response [%s]", sqlQueryId);
                    throw new RuntimeException(ex);
                  }
                  finally {
                    yielder.close();
                    lifecycle.emitLogsAndMetrics(e, remoteAddr, os.getCount());
                  }
                }
            )
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
          Status.INTERNAL_SERVER_ERROR.getStatusCode(),
          QueryInterruptedException.wrapIfNeeded(exceptionToReport)
      );
    }
    finally {
      Thread.currentThread().setName(currThreadName);
    }
  }
  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public Response doPost(
          final SqlQuery sqlQuery,
          @Context final HttpServletRequest req
  ) throws IOException
  {
    final SqlLifecycle lifecycle = sqlLifecycleFactory.factorize();
    final String sqlQueryId = lifecycle.initialize(sqlQuery.getQuery(), sqlQuery.getContext());
    final String remoteAddr = req.getRemoteAddr();
    final String currThreadName = Thread.currentThread().getName();

    try {
      Thread.currentThread().setName(StringUtils.format("sql[%s]", sqlQueryId));

      lifecycle.setParameters(sqlQuery.getParameterList());
      lifecycle.validateAndAuthorize(req);
      final PlannerContext plannerContext = lifecycle.plan();
      final DateTimeZone timeZone = plannerContext.getTimeZone();

      // Remember which columns are time-typed, so we can emit ISO8601 instead of millis values.
      // Also store list of all column names, for X-Druid-Sql-Columns header.
      final List<RelDataTypeField> fieldList = lifecycle.rowType().getFieldList();
      final boolean[] timeColumns = new boolean[fieldList.size()];
      final boolean[] dateColumns = new boolean[fieldList.size()];
      final boolean[] otherTypeColumns = new boolean[fieldList.size()];
      final String[] columnNames = new String[fieldList.size()];

      for (int i = 0; i < fieldList.size(); i++) {
        final SqlTypeName sqlTypeName = fieldList.get(i).getType().getSqlTypeName();
        timeColumns[i] = sqlTypeName == SqlTypeName.TIMESTAMP;
        dateColumns[i] = sqlTypeName == SqlTypeName.DATE;
        otherTypeColumns[i] = sqlTypeName == SqlTypeName.OTHER;
        columnNames[i] = fieldList.get(i).getName();
      }

      final Yielder<Object[]> yielder0 = Yielders.each(lifecycle.execute());

      List<Map<String,Object>> datas = toList(yielder0,fieldList,timeColumns,dateColumns,otherTypeColumns,timeZone);

      try {
        return Response
                .ok(
                        (StreamingOutput) outputStream -> {
                          Exception e = null;
                          CountingOutputStream os = new CountingOutputStream(outputStream);
                          Yielder<Object[]> yielder = yielder0;

                          try (final ResultFormat.Writer writer = sqlQuery.getResultFormat()
                                  .createFormatter(os, jsonMapper)) {
                            writer.writeResponseStart();

                            List<String> header;
                            if(datas.size() > 0){
                              header = Arrays.asList(datas.get(0).keySet().toArray(new String[datas.get(0).size()]));
                            }else {
                              header = Arrays.asList(columnNames);
                            }
                            if (sqlQuery.includeHeader()) {
                              writer.writeHeader(header);
                            }

                            for (Map<String,Object> data:datas){
                              writer.writeRowStart();
                              for (Map.Entry<String,Object> d:data.entrySet()){
                                writer.writeRowField(d.getKey(), d.getValue());
                              }
                              writer.writeRowEnd();
                            }
                            writer.writeResponseEnd();
                          }
                          catch (Exception ex) {
                            e = ex;
                            log.error(ex, "Unable to send SQL response [%s]", sqlQueryId);
                            throw new RuntimeException(ex);
                          }
                          finally {
                            yielder.close();
                            lifecycle.emitLogsAndMetrics(e, remoteAddr, os.getCount());
                          }
                        }
                )
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
              Status.INTERNAL_SERVER_ERROR.getStatusCode(),
              QueryInterruptedException.wrapIfNeeded(exceptionToReport)
      );
    }
    finally {
      Thread.currentThread().setName(currThreadName);
    }
  }

  List<Map<String,Object>> toList(Yielder<Object[]> yielder,
                                  final List<RelDataTypeField> fieldList,
                                  final boolean[] timeColumns,
                                  final boolean[] dateColumns,
                                  boolean[] otherTypeColumns,
                                  final DateTimeZone timeZone){
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
        cols.put(fieldList.get(i).getName(), value);
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
