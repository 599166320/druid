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

package org.apache.druid.server;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Ordering;
import com.google.inject.Inject;
import com.google.inject.Provider;
import org.apache.calcite.avatica.remote.ProtobufTranslation;
import org.apache.calcite.avatica.remote.ProtobufTranslationImpl;
import org.apache.calcite.avatica.remote.Service;
import org.apache.commons.io.IOUtils;
import org.apache.druid.client.selector.Server;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.guice.http.DruidHttpClientConfig;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.*;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.topn.TopNQuery;
import org.apache.druid.server.log.RequestLogger;
import org.apache.druid.server.metrics.QueryCountStatsProvider;
import org.apache.druid.server.router.QueryHostFinder;
import org.apache.druid.server.router.Router;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.Authenticator;
import org.apache.druid.server.security.AuthenticatorMapper;
import org.apache.druid.sql.http.SqlQuery;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.api.Response;
import org.eclipse.jetty.client.api.Result;
import org.eclipse.jetty.client.util.BytesContentProvider;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpMethod;
import org.eclipse.jetty.proxy.AsyncProxyServlet;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response.Status;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class does async query processing and should be merged with QueryResource at some point
 */
public class AsyncQueryForwardingServlet extends AsyncProxyServlet implements QueryCountStatsProvider
{
  private static final EmittingLogger log = new EmittingLogger(AsyncQueryForwardingServlet.class);
  @Deprecated // use SmileMediaTypes.APPLICATION_JACKSON_SMILE
  private static final String APPLICATION_SMILE = "application/smile";

  private static final String AVATICA_CONNECTION_ID = "connectionId";
  private static final String AVATICA_STATEMENT_HANDLE = "statementHandle";

  private static final String HOST_ATTRIBUTE = "org.apache.druid.proxy.to.host";
  private static final String SCHEME_ATTRIBUTE = "org.apache.druid.proxy.to.host.scheme";
  private static final String QUERY_ATTRIBUTE = "org.apache.druid.proxy.query";
  private static final String AVATICA_QUERY_ATTRIBUTE = "org.apache.druid.proxy.avaticaQuery";
  private static final String SQL_QUERY_ATTRIBUTE = "org.apache.druid.proxy.sqlQuery";
  private static final String OBJECTMAPPER_ATTRIBUTE = "org.apache.druid.proxy.objectMapper";

  private static final String PROPERTY_SQL_ENABLE = "druid.router.sql.enable";
  private static final String PROPERTY_SQL_ENABLE_DEFAULT = "false";

  private static final int CANCELLATION_TIMEOUT_MILLIS = 500;

  private final AtomicLong successfulQueryCount = new AtomicLong();
  private final AtomicLong failedQueryCount = new AtomicLong();
  private final AtomicLong interruptedQueryCount = new AtomicLong();

  private static void handleException(HttpServletResponse response, ObjectMapper objectMapper, Exception exception)
      throws IOException
  {
    if (!response.isCommitted()) {
      final String errorMessage = exception.getMessage() == null ? "null exception" : exception.getMessage();

      response.resetBuffer();
      response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
      objectMapper.writeValue(
          response.getOutputStream(),
          ImmutableMap.of("error", errorMessage)
      );
    }
    response.flushBuffer();
  }

  private final QueryToolChestWarehouse warehouse;
  private final ObjectMapper jsonMapper;
  private final ObjectMapper smileMapper;
  private final QueryHostFinder hostFinder;
  private final Provider<HttpClient> httpClientProvider;
  private final DruidHttpClientConfig httpClientConfig;
  private final ServiceEmitter emitter;
  private final RequestLogger requestLogger;
  private final GenericQueryMetricsFactory queryMetricsFactory;
  private final AuthenticatorMapper authenticatorMapper;
  private final ProtobufTranslation protobufTranslation;

  private final boolean routeSqlQueries;

  private HttpClient broadcastClient;

  @Inject
  public AsyncQueryForwardingServlet(
      QueryToolChestWarehouse warehouse,
      @Json ObjectMapper jsonMapper,
      @Smile ObjectMapper smileMapper,
      QueryHostFinder hostFinder,
      @Router Provider<HttpClient> httpClientProvider,
      @Router DruidHttpClientConfig httpClientConfig,
      ServiceEmitter emitter,
      RequestLogger requestLogger,
      GenericQueryMetricsFactory queryMetricsFactory,
      AuthenticatorMapper authenticatorMapper,
      Properties properties
  )
  {
    this.warehouse = warehouse;
    this.jsonMapper = jsonMapper;
    this.smileMapper = smileMapper;
    this.hostFinder = hostFinder;
    this.httpClientProvider = httpClientProvider;
    this.httpClientConfig = httpClientConfig;
    this.emitter = emitter;
    this.requestLogger = requestLogger;
    this.queryMetricsFactory = queryMetricsFactory;
    this.authenticatorMapper = authenticatorMapper;
    this.protobufTranslation = new ProtobufTranslationImpl();
    this.routeSqlQueries = Boolean.parseBoolean(
        properties.getProperty(PROPERTY_SQL_ENABLE, PROPERTY_SQL_ENABLE_DEFAULT)
    );
  }

  @Override
  public void init() throws ServletException
  {
    super.init();

    // Note that httpClientProvider is setup to return same HttpClient instance on each get() so
    // it is same http client as that is used by parent ProxyServlet.
    broadcastClient = newHttpClient();
    try {
      broadcastClient.start();
    }
    catch (Exception e) {
      throw new ServletException(e);
    }
  }

  @Override
  public void destroy()
  {
    super.destroy();
    try {
      broadcastClient.stop();
    }
    catch (Exception e) {
      log.warn(e, "Error stopping servlet");
    }
  }

  @Override
  protected void service(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException
  {
    final boolean isSmile = SmileMediaTypes.APPLICATION_JACKSON_SMILE.equals(request.getContentType())
                            || APPLICATION_SMILE.equals(request.getContentType());
    final ObjectMapper objectMapper = isSmile ? smileMapper : jsonMapper;
    request.setAttribute(OBJECTMAPPER_ATTRIBUTE, objectMapper);

    final String requestURI = request.getRequestURI();
    final String method = request.getMethod();
    final Server targetServer;

    // The Router does not have the ability to look inside SQL queries and route them intelligently, so just treat
    // them as a generic request.
    final boolean isNativeQueryEndpoint = requestURI.startsWith("/druid/v2") && !requestURI.startsWith("/druid/v2/sql");
    final boolean isSqlQueryEndpoint = requestURI.startsWith("/druid/v2/sql");
    final boolean isPromqlQueryEndpoint = requestURI.startsWith("/api/v1");

    final boolean isAvaticaJson = requestURI.startsWith("/druid/v2/sql/avatica");
    final boolean isAvaticaPb = requestURI.startsWith("/druid/v2/sql/avatica-protobuf");

    if (isAvaticaPb) {
      byte[] requestBytes = IOUtils.toByteArray(request.getInputStream());
      Service.Request protobufRequest = this.protobufTranslation.parseRequest(requestBytes);
      String connectionId = getAvaticaProtobufConnectionId(protobufRequest);
      targetServer = hostFinder.findServerAvatica(connectionId);
      request.setAttribute(AVATICA_QUERY_ATTRIBUTE, requestBytes);
    } else if (isAvaticaJson) {
      Map<String, Object> requestMap = objectMapper.readValue(
          request.getInputStream(),
          JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
      );
      String connectionId = getAvaticaConnectionId(requestMap);
      targetServer = hostFinder.findServerAvatica(connectionId);
      byte[] requestBytes = objectMapper.writeValueAsBytes(requestMap);
      request.setAttribute(AVATICA_QUERY_ATTRIBUTE, requestBytes);
    } else if (isNativeQueryEndpoint && HttpMethod.DELETE.is(method)) {
      // query cancellation request
      targetServer = hostFinder.pickDefaultServer();
      broadcastQueryCancelRequest(request, targetServer);
    } else if (isNativeQueryEndpoint && HttpMethod.POST.is(method)) {
      // query request
      byte [] datas = getRequestPostBytes(request);
      try {
        //Query inputQuery = objectMapper.readValue(request.getInputStream(), Query.class);
        Query inputQuery = objectMapper.readValue(datas, Query.class);
        if (inputQuery != null) {
          targetServer = hostFinder.pickServer(inputQuery);
          if (inputQuery.getId() == null) {
            inputQuery = inputQuery.withId(UUID.randomUUID().toString());
          }
        } else {
          targetServer = hostFinder.pickDefaultServer();
        }
        request.setAttribute(QUERY_ATTRIBUTE, inputQuery);
      }
      catch (IOException e) {
        try {
          view materializedViewQuery = objectMapper.readValue(datas, view.class);
          materializedViewQuery.setQueryType("view");
          request.setAttribute(QUERY_ATTRIBUTE, materializedViewQuery);
          Query inputQuery = materializedViewQuery.getQuery();
          Server targetServer1;
          if (inputQuery != null) {
            targetServer1 = hostFinder.pickServer(inputQuery);
            if (inputQuery.getId() == null) {
              inputQuery.withId(UUID.randomUUID().toString());
            }
          } else {
            targetServer1 = hostFinder.pickDefaultServer();
          }
          request.setAttribute(HOST_ATTRIBUTE, targetServer1.getHost());
          request.setAttribute(SCHEME_ATTRIBUTE, targetServer1.getScheme());
          doService(request, response);
          return;
        }catch (Exception e1){
          handleQueryParseException(request, response, objectMapper, e, true);
          return;
        }
      }
      catch (Exception e) {
        handleException(response, objectMapper, e);
        return;
      }
    } else if (routeSqlQueries && isSqlQueryEndpoint && HttpMethod.POST.is(method)) {
      try {
        SqlQuery inputSqlQuery = objectMapper.readValue(request.getInputStream(), SqlQuery.class);
        request.setAttribute(SQL_QUERY_ATTRIBUTE, inputSqlQuery);
        targetServer = hostFinder.findServerSql(inputSqlQuery);
      }
      catch (IOException e) {
        handleQueryParseException(request, response, objectMapper, e, false);
        return;
      }
      catch (Exception e) {
        handleException(response, objectMapper, e);
        return;
      }
    }else if(isPromqlQueryEndpoint &&  HttpMethod.POST.is(method)){
      targetServer = hostFinder.pickDefaultServer();
    }
    else {
      targetServer = hostFinder.pickDefaultServer();
    }

    request.setAttribute(HOST_ATTRIBUTE, targetServer.getHost());
    request.setAttribute(SCHEME_ATTRIBUTE, targetServer.getScheme());

    doService(request, response);
  }

  /**
   * Issues async query cancellation requests to all Brokers (except the given
   * targetServer). Query cancellation on the targetServer is handled by the
   * proxy servlet.
   */
  private void broadcastQueryCancelRequest(HttpServletRequest request, Server targetServer)
  {
    // send query cancellation to all brokers this query may have gone to
    // to keep the code simple, the proxy servlet will also send a request to the default targetServer.
    for (final Server server : hostFinder.getAllServers()) {
      if (server.getHost().equals(targetServer.getHost())) {
        continue;
      }

      // issue async requests
      Response.CompleteListener completeListener = result -> {
        if (result.isFailed()) {
          log.warn(
              result.getFailure(),
              "Failed to forward cancellation request to [%s]",
              server.getHost()
          );
        }
      };

      Request broadcastReq = broadcastClient
          .newRequest(rewriteURI(request, server.getScheme(), server.getHost()))
          .method(HttpMethod.DELETE)
          .timeout(CANCELLATION_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);

      copyRequestHeaders(request, broadcastReq);
      broadcastReq.send(completeListener);
    }

    interruptedQueryCount.incrementAndGet();
  }

  private void handleQueryParseException(
      HttpServletRequest request,
      HttpServletResponse response,
      ObjectMapper objectMapper,
      IOException parseException,
      boolean isNativeQuery
  ) throws IOException
  {
    log.warn(parseException, "Exception parsing query");

    // Log the error message
    final String errorMessage = parseException.getMessage() == null
                                ? "no error message" : parseException.getMessage();
    if (isNativeQuery) {
      requestLogger.logNativeQuery(
          RequestLogLine.forNative(
              null,
              DateTimes.nowUtc(),
              request.getRemoteAddr(),
              new QueryStats(ImmutableMap.of("success", false, "exception", errorMessage))
          )
      );
    } else {
      requestLogger.logSqlQuery(
          RequestLogLine.forSql(
              null,
              null,
              DateTimes.nowUtc(),
              request.getRemoteAddr(),
              new QueryStats(ImmutableMap.of("success", false, "exception", errorMessage))
          )
      );
    }

    // Write to the response
    response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
    response.setContentType(MediaType.APPLICATION_JSON);
    objectMapper.writeValue(
        response.getOutputStream(),
        ImmutableMap.of("error", errorMessage)
    );
  }

  protected void doService(
      HttpServletRequest request,
      HttpServletResponse response
  ) throws ServletException, IOException
  {
    // Just call the superclass service method. Overriden in tests.
    super.service(request, response);
  }

  @Override
  protected void sendProxyRequest(
      HttpServletRequest clientRequest,
      HttpServletResponse proxyResponse,
      Request proxyRequest
  )
  {
    proxyRequest.timeout(httpClientConfig.getReadTimeout().getMillis(), TimeUnit.MILLISECONDS);
    proxyRequest.idleTimeout(httpClientConfig.getReadTimeout().getMillis(), TimeUnit.MILLISECONDS);

    byte[] avaticaQuery = (byte[]) clientRequest.getAttribute(AVATICA_QUERY_ATTRIBUTE);
    if (avaticaQuery != null) {
      proxyRequest.content(new BytesContentProvider(avaticaQuery));
    }

    final Query query = (Query) clientRequest.getAttribute(QUERY_ATTRIBUTE);
    final SqlQuery sqlQuery = (SqlQuery) clientRequest.getAttribute(SQL_QUERY_ATTRIBUTE);
    if (query != null) {
      setProxyRequestContent(proxyRequest, clientRequest, query);
    } else if (sqlQuery != null) {
      setProxyRequestContent(proxyRequest, clientRequest, sqlQuery);
    }

    // Since we can't see the request object on the remote side, we can't check whether the remote side actually
    // performed an authorization check here, so always set this to true for the proxy servlet.
    // If the remote node failed to perform an authorization check, PreResponseAuthorizationCheckFilter
    // will log that on the remote node.
    clientRequest.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, true);

    // Check if there is an authentication result and use it to decorate the proxy request if needed.
    AuthenticationResult authenticationResult = (AuthenticationResult) clientRequest.getAttribute(
        AuthConfig.DRUID_AUTHENTICATION_RESULT);
    if (authenticationResult != null && authenticationResult.getAuthenticatedBy() != null) {
      Authenticator authenticator = authenticatorMapper.getAuthenticatorMap()
                                                       .get(authenticationResult.getAuthenticatedBy());
      if (authenticator != null) {
        authenticator.decorateProxyRequest(
            clientRequest,
            proxyResponse,
            proxyRequest
        );
      } else {
        log.error("Can not find Authenticator with Name [%s]", authenticationResult.getAuthenticatedBy());
      }
    }
    super.sendProxyRequest(
        clientRequest,
        proxyResponse,
        proxyRequest
    );
  }

  private void setProxyRequestContent(Request proxyRequest, HttpServletRequest clientRequest, Object content)
  {
    final ObjectMapper objectMapper = (ObjectMapper) clientRequest.getAttribute(OBJECTMAPPER_ATTRIBUTE);
    try {
      byte[] bytes = objectMapper.writeValueAsBytes(content);
      proxyRequest.content(new BytesContentProvider(bytes));
      proxyRequest.getHeaders().put(HttpHeader.CONTENT_LENGTH, String.valueOf(bytes.length));
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected Response.Listener newProxyResponseListener(HttpServletRequest request, HttpServletResponse response)
  {
    final Query query = (Query) request.getAttribute(QUERY_ATTRIBUTE);
    if (query != null) {
      return newMetricsEmittingProxyResponseListener(request, response, query, System.nanoTime());
    } else {
      return super.newProxyResponseListener(request, response);
    }
  }

  @Override
  protected String rewriteTarget(HttpServletRequest request)
  {
    return rewriteURI(
        request,
        (String) request.getAttribute(SCHEME_ATTRIBUTE),
        (String) request.getAttribute(HOST_ATTRIBUTE)
    );
  }

  protected String rewriteURI(HttpServletRequest request, String scheme, String host)
  {
    return makeURI(scheme, host, request.getRequestURI(), request.getQueryString());
  }

  @VisibleForTesting
  static String makeURI(String scheme, String host, String requestURI, String rawQueryString)
  {
    return JettyUtils.concatenateForRewrite(
        scheme + "://" + host,
        requestURI,
        rawQueryString
    );
  }

  @Override
  protected HttpClient newHttpClient()
  {
    return httpClientProvider.get();
  }

  @Override
  protected HttpClient createHttpClient() throws ServletException
  {
    HttpClient client = super.createHttpClient();
    // override timeout set in ProxyServlet.createHttpClient
    setTimeout(httpClientConfig.getReadTimeout().getMillis());
    return client;
  }

  private Response.Listener newMetricsEmittingProxyResponseListener(
      HttpServletRequest request,
      HttpServletResponse response,
      Query query,
      long startNs
  )
  {
    return new MetricsEmittingProxyResponseListener(request, response, query, startNs);
  }

  @Override
  public long getSuccessfulQueryCount()
  {
    return successfulQueryCount.get();
  }

  @Override
  public long getFailedQueryCount()
  {
    return failedQueryCount.get();
  }

  @Override
  public long getInterruptedQueryCount()
  {
    return interruptedQueryCount.get();
  }

  @Override
  public long getTimedOutQueryCount()
  {
    // Query timeout metric is not relevant here and this metric is already being tracked in the Broker and the
    // data nodes using QueryResource
    return 0L;
  }

  @VisibleForTesting
  static String getAvaticaConnectionId(Map<String, Object> requestMap)
  {
    // avatica commands always have a 'connectionId'. If commands are not part of a prepared statement, this appears at
    // the top level of the request, but if it is part of a statement, then it will be nested in the 'statementHandle'.
    // see https://calcite.apache.org/avatica/docs/json_reference.html#requests for more details
    Object connectionIdObj = requestMap.get(AVATICA_CONNECTION_ID);
    if (connectionIdObj == null) {
      Object statementHandle = requestMap.get(AVATICA_STATEMENT_HANDLE);
      if (statementHandle != null && statementHandle instanceof Map) {
        connectionIdObj = ((Map) statementHandle).get(AVATICA_CONNECTION_ID);
      }
    }

    if (connectionIdObj == null) {
      throw new IAE("Received an Avatica request without a %s.", AVATICA_CONNECTION_ID);
    }
    if (!(connectionIdObj instanceof String)) {
      throw new IAE("Received an Avatica request with a non-String %s.", AVATICA_CONNECTION_ID);
    }

    return (String) connectionIdObj;
  }

  static String getAvaticaProtobufConnectionId(Service.Request request)
  {
    if (request instanceof Service.CatalogsRequest) {
      return ((Service.CatalogsRequest) request).connectionId;
    }

    if (request instanceof Service.SchemasRequest) {
      return ((Service.SchemasRequest) request).connectionId;
    }

    if (request instanceof Service.TablesRequest) {
      return ((Service.TablesRequest) request).connectionId;
    }

    if (request instanceof Service.TypeInfoRequest) {
      return ((Service.TypeInfoRequest) request).connectionId;
    }

    if (request instanceof Service.ColumnsRequest) {
      return ((Service.ColumnsRequest) request).connectionId;
    }

    if (request instanceof Service.ExecuteRequest) {
      return ((Service.ExecuteRequest) request).statementHandle.connectionId;
    }

    if (request instanceof Service.TableTypesRequest) {
      return ((Service.TableTypesRequest) request).connectionId;
    }

    if (request instanceof Service.PrepareRequest) {
      return ((Service.PrepareRequest) request).connectionId;
    }

    if (request instanceof Service.PrepareAndExecuteRequest) {
      return ((Service.PrepareAndExecuteRequest) request).connectionId;
    }

    if (request instanceof Service.FetchRequest) {
      return ((Service.FetchRequest) request).connectionId;
    }

    if (request instanceof Service.CreateStatementRequest) {
      return ((Service.CreateStatementRequest) request).connectionId;
    }

    if (request instanceof Service.CloseStatementRequest) {
      return ((Service.CloseStatementRequest) request).connectionId;
    }

    if (request instanceof Service.OpenConnectionRequest) {
      return ((Service.OpenConnectionRequest) request).connectionId;
    }

    if (request instanceof Service.CloseConnectionRequest) {
      return ((Service.CloseConnectionRequest) request).connectionId;
    }

    if (request instanceof Service.ConnectionSyncRequest) {
      return ((Service.ConnectionSyncRequest) request).connectionId;
    }

    if (request instanceof Service.DatabasePropertyRequest) {
      return ((Service.DatabasePropertyRequest) request).connectionId;
    }

    if (request instanceof Service.SyncResultsRequest) {
      return ((Service.SyncResultsRequest) request).connectionId;
    }

    if (request instanceof Service.CommitRequest) {
      return ((Service.CommitRequest) request).connectionId;
    }

    if (request instanceof Service.RollbackRequest) {
      return ((Service.RollbackRequest) request).connectionId;
    }

    if (request instanceof Service.PrepareAndExecuteBatchRequest) {
      return ((Service.PrepareAndExecuteBatchRequest) request).connectionId;
    }

    if (request instanceof Service.ExecuteBatchRequest) {
      return ((Service.ExecuteBatchRequest) request).connectionId;
    }

    throw new IAE("Received an unknown Avatica protobuf request");
  }

  private class MetricsEmittingProxyResponseListener<T> extends ProxyResponseListener
  {
    private final HttpServletRequest req;
    private final HttpServletResponse res;
    private final Query<T> query;
    private final long startNs;

    public MetricsEmittingProxyResponseListener(
        HttpServletRequest request,
        HttpServletResponse response,
        Query<T> query,
        long startNs
    )
    {
      super(request, response);

      this.req = request;
      this.res = response;
      this.query = query;
      this.startNs = startNs;
    }

    @Override
    public void onComplete(Result result)
    {
      final long requestTimeNs = System.nanoTime() - startNs;
      try {
        boolean success = result.isSucceeded();
        if (success) {
          successfulQueryCount.incrementAndGet();
        } else {
          failedQueryCount.incrementAndGet();
        }
        emitQueryTime(requestTimeNs, success);
        requestLogger.logNativeQuery(
            RequestLogLine.forNative(
                query,
                DateTimes.nowUtc(),
                req.getRemoteAddr(),
                new QueryStats(
                    ImmutableMap.of(
                        "query/time",
                        TimeUnit.NANOSECONDS.toMillis(requestTimeNs),
                        "success",
                        success
                        && result.getResponse().getStatus() == Status.OK.getStatusCode()
                    )
                )
            )
        );
      }
      catch (Exception e) {
        log.error(e, "Unable to log query [%s]!", query);
      }

      super.onComplete(result);
    }

    @Override
    public void onFailure(Response response, Throwable failure)
    {
      try {
        final String errorMessage = failure.getMessage();
        failedQueryCount.incrementAndGet();
        emitQueryTime(System.nanoTime() - startNs, false);
        requestLogger.logNativeQuery(
            RequestLogLine.forNative(
                query,
                DateTimes.nowUtc(),
                req.getRemoteAddr(),
                new QueryStats(
                    ImmutableMap.of(
                        "success",
                        false,
                        "exception",
                        errorMessage == null ? "no message" : errorMessage
                    )
                )
            )
        );
      }
      catch (IOException logError) {
        log.error(logError, "Unable to log query [%s]!", query);
      }

      log.makeAlert(failure, "Exception handling request")
         .addData("exception", failure.toString())
         .addData("query", query)
         .addData("peer", req.getRemoteAddr())
         .emit();

      super.onFailure(response, failure);
    }

    private void emitQueryTime(long requestTimeNs, boolean success)
    {
      QueryMetrics queryMetrics = DruidMetrics.makeRequestMetrics(
          queryMetricsFactory,
          warehouse.getToolChest(query),
          query,
          req.getRemoteAddr()
      );
      queryMetrics.success(success);
      queryMetrics.reportQueryTime(requestTimeNs).emit(emitter);
    }
  }
  private static byte[] getRequestPostBytes(HttpServletRequest request) throws IOException {
    int contentLength = request.getContentLength();
    if(contentLength<0){
      return null;
    }
    byte[] buffer = new byte[contentLength];
    for (int i = 0; i < contentLength;) {
      int readlen = request.getInputStream().read(buffer, i,
              contentLength - i);
      if (readlen == -1) {
        break;
      }
      i += readlen;
    }
    return buffer;
  }
}
class view implements Query{
  public String queryType;
  private Query query;
  @JsonCreator
  public view(
          @JsonProperty("query") Query query,
          @JsonProperty("queryType") String queryType
  )
  {
    Preconditions.checkArgument(
            query instanceof TopNQuery || query instanceof TimeseriesQuery || query instanceof GroupByQuery,
            "Only topN/timeseries/groupby query are supported"
    );
    this.query = query;
    this.queryType = queryType;
  }

  public void setQueryType(String queryType) {
    this.queryType = queryType;
  }

  public void setQuery(Query query) {
    this.query = query;
  }

  @JsonProperty("queryType")
  public String getQueryType() {
    return queryType;
  }
  @JsonProperty("query")
  public Query getQuery() {
    return query;
  }

  @Override
  public DataSource getDataSource() {
    return query.getDataSource();
  }

  @Override
  public boolean hasFilters() {
    return query.hasFilters();
  }

  @Override
  public DimFilter getFilter() {
    return query.getFilter();
  }

  @Override
  public String getType() {
    return query.getType();
  }

  @Override
  public QueryRunner getRunner(QuerySegmentWalker walker) {
    return query.getRunner(walker);
  }

  @Override
  public List<Interval> getIntervals() {
    return query.getIntervals();
  }

  @Override
  public Duration getDuration() {
    return query.getDuration();
  }

  @Override
  public Granularity getGranularity() {
    return query.getGranularity();
  }

  @Override
  public DateTimeZone getTimezone() {
    return null;
  }

  @Override
  public Map<String, Object> getContext() {
    return query.getContext();
  }

  @Override
  public boolean getContextBoolean(String key, boolean defaultValue) {
    return query.getContextBoolean(key,defaultValue);
  }

  @Override
  public boolean isDescending() {
    return query.isDescending();
  }

  @Override
  public Ordering getResultOrdering() {
    return query.getResultOrdering();
  }

  @Override
  public Query withQuerySegmentSpec(QuerySegmentSpec spec) {
    return query.withQuerySegmentSpec(spec);
  }

  @Override
  public Query withId(String id) {
    return query.withId(id);
  }

  @Nullable
  @Override
  public String getId() {
    return query.getId();
  }

  @Override
  public Query withSubQueryId(String subQueryId) {
    return query.withSubQueryId(subQueryId);
  }

  @Nullable
  @Override
  public String getSubQueryId() {
    return query.getSubQueryId();
  }

  @Override
  public Query withDataSource(DataSource dataSource) {
    return query.withDataSource(dataSource);
  }

  @Override
  public Query withOverriddenContext(Map contextOverride) {
    return query.withOverriddenContext(contextOverride);
  }

  @Override
  public Object getContextValue(String key, Object defaultValue) {
    return query.getContextValue(key,defaultValue);
  }

  @Override
  public Object getContextValue(String key) {
    return query.getContextValue(key);
  }
}