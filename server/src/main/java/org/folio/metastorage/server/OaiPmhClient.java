package org.folio.metastorage.server;

import io.netty.handler.codec.http.QueryStringEncoder;
import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.RequestOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.validation.RequestParameters;
import io.vertx.ext.web.validation.ValidationHandler;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowIterator;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.Tuple;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.metastorage.util.OaiMetadataParser;
import org.folio.metastorage.util.OaiMetadataParserMarcInJson;
import org.folio.metastorage.util.OaiParserStream;
import org.folio.metastorage.util.OaiRecord;
import org.folio.metastorage.util.SourceId;
import org.folio.metastorage.util.XmlParser;
import org.folio.okapi.common.HttpResponse;

public class OaiPmhClient {

  Vertx vertx;

  HttpClient httpClient;

  private static final String STATUS_LITERAL = "status";

  private static final String RESUMPTION_TOKEN_LITERAL = "resumptionToken";

  private static final String IDLE_LITERAL = "idle";

  private static final String RUNNING_LITERAL = "running";

  private static final String CONFIG_LITERAL = "config";

  private static final String TOTAL_RECORDS_LITERAL = "totalRecords";

  private static final String TOTAL_REQUESTS_LITERAL = "totalRequests";

  private static final Logger log = LogManager.getLogger(OaiPmhClient.class);

  public OaiPmhClient(Vertx vertx) {
    this.vertx = vertx;
    this.httpClient = vertx.createHttpClient();
  }

  /**
   * Create OAI-PMH client.
   *
   * @param ctx routing context
   * @return async result
   */
  public Future<Void> post(RoutingContext ctx) {
    Storage storage = new Storage(ctx);
    JsonObject config = ctx.getBodyAsJson();

    String id = config.getString("id");
    config.remove("id");
    return storage.getPool().preparedQuery("INSERT INTO " + storage.getOaiPmhClientTable()
            + " (id, config)"
            + " VALUES ($1, $2)")
        .execute(Tuple.of(id, config))
        .map(x -> {
          HttpResponse.responseJson(ctx, 201).end(config.put("id", id).encode());
          return null;
        });
  }

  static Future<Row> getOaiPmhClient(Storage storage, SqlConnection connection, String id) {
    return connection.preparedQuery("SELECT * FROM " + storage.getOaiPmhClientTable()
            + " WHERE id = $1")
        .execute(Tuple.of(id))
        .map(rowSet -> {
          RowIterator<Row> iterator = rowSet.iterator();
          if (!iterator.hasNext()) {
            return null;
          }
          return iterator.next();
        });
  }

  static Future<JsonObject> getConfig(Storage storage, String id) {
    return storage.getPool().withConnection(connection ->
        getOaiPmhClient(storage, connection, id).map(row -> {
          if (row == null) {
            return null;
          }
          return row.getJsonObject(CONFIG_LITERAL);
        }));
  }

  static Future<JsonObject> getJob(Storage storage, SqlConnection connection, String id) {
    return getOaiPmhClient(storage, connection, id).map(row -> {
      if (row == null) {
        return null;
      }
      JsonObject job = row.getJsonObject("job");
      if (job == null) {
        job = new JsonObject()
            .put(STATUS_LITERAL, IDLE_LITERAL)
            .put(TOTAL_RECORDS_LITERAL, 0L)
            .put(TOTAL_REQUESTS_LITERAL, 0L);
      }
      JsonObject config = job.getJsonObject(CONFIG_LITERAL);
      if (config == null) {
        config = row.getJsonObject(CONFIG_LITERAL);
      }
      config.put("id", id);
      job.put(CONFIG_LITERAL, config);
      return job;
    });
  }

  /**
   * Get OAI-PMH client.
   *
   * @param ctx routing context
   * @return async result
   */
  public Future<Void> get(RoutingContext ctx) {
    Storage storage = new Storage(ctx);
    RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
    String id = Util.getParameterString(params.pathParameter("id"));
    return getConfig(storage, id).map(config -> {
      if (config == null) {
        HttpResponse.responseError(ctx, 404, id);
        return null;
      }
      config.put("id", id);
      HttpResponse.responseJson(ctx, 200).end(config.encode());
      return null;
    });
  }

  /**
   * Get all OAI-PMH clients.
   *
   * @param ctx routing context
   * @return async result
   */
  public Future<Void> getCollection(RoutingContext ctx) {
    Storage storage = new Storage(ctx);
    return storage.getPool().query("SELECT id,config FROM " + storage.getOaiPmhClientTable())
        .execute()
        .map(rowSet -> {
          JsonArray ar = new JsonArray();
          rowSet.forEach(x -> {
            JsonObject config = x.getJsonObject(CONFIG_LITERAL);
            config.put("id", x.getValue("id"));
            ar.add(config);
          });
          JsonObject response = new JsonObject();
          response.put("items", ar);
          response.put("resultInfo", new JsonObject().put(TOTAL_RECORDS_LITERAL, ar.size()));
          HttpResponse.responseJson(ctx, 200).end(response.encode());
          return null;
        });
  }

  /**
   * Delete OAI-PMH client.
   *
   * @param ctx routing context
   * @return async result
   */
  public Future<Void> delete(RoutingContext ctx) {
    Storage storage = new Storage(ctx);
    RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
    String id = Util.getParameterString(params.pathParameter("id"));
    return storage.getPool().preparedQuery("DELETE FROM " + storage.getOaiPmhClientTable()
            + " WHERE id = $1")
        .execute(Tuple.of(id))
        .map(rowSet -> {
          if (rowSet.rowCount() == 0) {
            HttpResponse.responseError(ctx, 404, id);
          } else {
            ctx.response().setStatusCode(204).end();
          }
          return null;
        });
  }

  /**
   * Update OAI-PMH client.
   *
   * @param ctx routing context
   * @return async result
   */
  public Future<Void> put(RoutingContext ctx) {
    Storage storage = new Storage(ctx);
    RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
    String id = Util.getParameterString(params.pathParameter("id"));
    JsonObject config = ctx.getBodyAsJson();
    config.remove("id");
    return storage.getPool().preparedQuery("UPDATE " + storage.getOaiPmhClientTable()
            + " SET config = $2 WHERE id = $1")
        .execute(Tuple.of(id, config))
        .map(rowSet -> {
          if (rowSet.rowCount() == 0) {
            HttpResponse.responseError(ctx, 404, id);
          } else {
            ctx.response().setStatusCode(204).end();
          }
          return null;
        });
  }

  static Future<Void> updateJob(
      Storage storage, SqlConnection connection, String id,
      JsonObject job) {
    return connection.preparedQuery("UPDATE " + storage.getOaiPmhClientTable()
            + " SET job = $2 WHERE id = $1")
        .execute(Tuple.of(id, job))
        .mapEmpty();
  }

  static Future<Boolean> lock(SqlConnection connection) {
    return connection.preparedQuery("SELECT pg_try_advisory_lock($1)")
        .execute(Tuple.of(1))
        .map(rowSet -> rowSet.iterator().next().getBoolean(0));
  }


  /**
   * Start OAI PMH client job.
   *
   * @param ctx routing context
   * @return async result
   */
  public Future<Void> start(RoutingContext ctx) {
    Storage storage = new Storage(ctx);
    RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
    String id = Util.getParameterString(params.pathParameter("id"));

    return storage.pool.getConnection().compose(connection ->
        getJob(storage, connection, id).compose(job -> {
          if (job == null) {
            HttpResponse.responseError(ctx, 404, id);
            return connection.close();
          }
          return lock(connection)
              .compose(x -> {
                if (Boolean.FALSE.equals(x)) {
                  HttpResponse.responseError(ctx, 400, "already locked");
                  return connection.close();
                }
                job.put(STATUS_LITERAL, RUNNING_LITERAL);
                return updateJob(storage, connection, id, job)
                    .onFailure(e -> connection.close())
                    .map(y -> {
                      ctx.response().setStatusCode(204).end();
                      oaiHarvestLoop(storage, connection, id, job);
                      return null;
                    });
              })
              .mapEmpty();
        })
    );
  }

  /**
   * Stop OAI PMH client job.
   *
   * @param ctx routing context
   * @return async result
   */
  public Future<Void> stop(RoutingContext ctx) {
    Storage storage = new Storage(ctx);
    RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
    String id = Util.getParameterString(params.pathParameter("id"));

    return storage.pool.withConnection(connection ->
        getJob(storage, connection, id)
            .compose(job -> {
              if (job == null) {
                HttpResponse.responseError(ctx, 404, id);
                return Future.succeededFuture();
              }
              String status = job.getString(STATUS_LITERAL);
              if (IDLE_LITERAL.equals(status)) {
                HttpResponse.responseError(ctx, 400, "not running");
                return Future.succeededFuture();
              }
              job.put(STATUS_LITERAL, IDLE_LITERAL);
              return updateJob(storage, connection, id, job)
                  .map(x -> {
                    ctx.response().setStatusCode(204).end();
                    return null;
                  });
            }));
  }

  /**
   * Get OAI PMH client status.
   *
   * @param ctx routing context
   * @return async result
   */
  public Future<Void> status(RoutingContext ctx) {
    Storage storage = new Storage(ctx);
    RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
    String id = Util.getParameterString(params.pathParameter("id"));

    return storage.pool.withConnection(connection ->
        getJob(storage, connection, id)
            .map(job -> {
              if (job == null) {
                HttpResponse.responseError(ctx, 404, id);
                return null;
              }
              HttpResponse.responseJson(ctx, 200).end(job.encode());
              return null;
            }));
  }

  static MultiMap getHttpHeaders(JsonObject config) {
    MultiMap headers = MultiMap.caseInsensitiveMultiMap();
    headers.add("Accept", "text/xml");
    JsonObject userHeaders = config.getJsonObject("headers");
    if (userHeaders != null) {
      userHeaders.forEach(e -> {
        if (e.getValue() instanceof String value) {
          headers.add(e.getKey(), value);
        } else {
          throw new IllegalArgumentException("headers " + e.getKey() + " value must be string");
        }
      });
    }
    return headers;
  }

  static boolean addQueryParameterFromConfig(QueryStringEncoder enc,
      JsonObject config, String key) {
    String value = config.getString(key);
    if (value == null) {
      return false;
    }
    enc.addParam(key, value);
    return true;
  }

  static void addQueryParameterFromParams(QueryStringEncoder enc, JsonObject params) {
    if (params != null) {
      params.forEach(e -> {
        if (e.getValue() instanceof String value) {
          enc.addParam(e.getKey(), value);
        } else {
          throw new IllegalArgumentException("params " + e.getKey() + " value must be string");
        }
      });
    }
  }

  Future<Void> ingestRecord(
      Storage storage, OaiRecord<JsonObject> oaiRecord,
      SourceId sourceId, JsonArray matchKeyConfigs) {
    try {
      JsonObject globalRecord = new JsonObject();
      globalRecord.put("localId", oaiRecord.getIdentifier());
      if (oaiRecord.isDeleted()) {
        globalRecord.put("delete", true);
      } else {
        globalRecord.put("payload", new JsonObject().put("marc", oaiRecord.getMetadata()));
      }
      return storage.ingestGlobalRecord(vertx, sourceId, globalRecord, matchKeyConfigs);
    } catch (Exception e) {
      log.error("{}", e.getMessage(), e);
      return Future.failedFuture(e);
    }
  }

  static class Datestamp {
    String value;
  }

  void oaiHarvestLoop(Storage storage, SqlConnection connection, String id, JsonObject job) {
    JsonObject config = job.getJsonObject(CONFIG_LITERAL);
    RequestOptions requestOptions = new RequestOptions();
    requestOptions.setMethod(HttpMethod.GET);
    requestOptions.setHeaders(getHttpHeaders(config));
    QueryStringEncoder enc = new QueryStringEncoder(config.getString("url"));
    enc.addParam("verb", "ListRecords");
    if (!addQueryParameterFromConfig(enc, config, RESUMPTION_TOKEN_LITERAL)) {
      addQueryParameterFromConfig(enc, config, "from");
      addQueryParameterFromConfig(enc, config, "until");
      addQueryParameterFromConfig(enc, config, "set");
      addQueryParameterFromConfig(enc, config, "metadataPrefix");
    }
    addQueryParameterFromParams(enc, config.getJsonObject("params"));
    log.info("client send request {}", enc.toString());
    requestOptions.setAbsoluteURI(enc.toString());

    storage.getAvailableMatchConfigs(connection)
        .compose(matchKeyConfigs ->
            httpClient.request(requestOptions)
                .compose(HttpClientRequest::send)
                .compose(res -> {
                  log.info("client handle response");
                  job.put(TOTAL_REQUESTS_LITERAL, job.getLong(TOTAL_REQUESTS_LITERAL) + 1);
                  if (res.statusCode() != 200) {
                    job.put(STATUS_LITERAL, IDLE_LITERAL);
                    job.put("errors", "OAI server returned HTTP status " + res.statusCode());
                    return updateJob(storage, connection, id, job)
                        .compose(x -> Future.failedFuture("stopping due to HTTP status error"));
                  }
                  XmlParser xmlParser = XmlParser.newParser(res);
                  Promise<Void> promise = Promise.promise();
                  OaiMetadataParser<JsonObject> metadataParser = new OaiMetadataParserMarcInJson();
                  SourceId sourceId = new SourceId(config.getString("sourceId"));
                  Datestamp newestDatestamp = new Datestamp();
                  AtomicInteger cnt = new AtomicInteger();
                  OaiParserStream<JsonObject> oaiParserStream =
                      new OaiParserStream<>(xmlParser,
                          oaiRecord -> {
                            cnt.incrementAndGet();
                            String datestamp = oaiRecord.getDatestamp();
                            if (newestDatestamp.value == null
                                || datestamp.compareTo(newestDatestamp.value) > 0) {
                              newestDatestamp.value = datestamp;
                            }
                            xmlParser.pause();
                            ingestRecord(storage, oaiRecord, sourceId, matchKeyConfigs)
                                .onComplete(x -> xmlParser.resume());
                          },
                          metadataParser);
                  oaiParserStream.exceptionHandler(promise::fail);
                  xmlParser.endHandler(end -> {
                    job.put(TOTAL_RECORDS_LITERAL, job.getLong(TOTAL_RECORDS_LITERAL) + cnt.get());
                    log.info("ingested {} records", cnt.get());
                    if (newestDatestamp.value != null) {
                      config.put("from", Util.getNextOaiDate(newestDatestamp.value));
                    }
                    String resumptionToken = oaiParserStream.getResumptionToken();
                    String oldResumptionToken = config.getString(RESUMPTION_TOKEN_LITERAL);
                    if (resumptionToken == null
                        || resumptionToken.equals(oldResumptionToken)) {
                      config.remove(RESUMPTION_TOKEN_LITERAL);
                      job.put(STATUS_LITERAL, IDLE_LITERAL);
                      updateJob(storage, connection, id, job)
                          .onComplete(e -> promise.fail("stopping due to no resumptionToken"));
                    } else {
                      config.put(RESUMPTION_TOKEN_LITERAL, resumptionToken);
                      log.info("continuing with resumptionToken");
                      updateJob(storage, connection, id, job).onComplete(promise);
                    }
                  });
                  return promise.future();
                }))
        .onSuccess(x -> oaiHarvestLoop(storage, connection, id, job))
        .onFailure(e -> {
          log.error(e.getMessage(), e);
          connection.close();
        });
  }
}
