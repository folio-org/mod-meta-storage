package org.folio.metastorage.server;

import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.validation.RequestParameters;
import io.vertx.ext.web.validation.ValidationHandler;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowIterator;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.Tuple;
import java.io.ByteArrayInputStream;
import java.util.LinkedList;
import java.util.List;
import javax.xml.stream.XMLStreamException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.metastorage.util.OaiParser;
import org.folio.metastorage.util.OaiRecord;
import org.folio.metastorage.util.SourceId;
import org.folio.metastorage.util.XmlJsonUtil;
import org.folio.okapi.common.GenericCompositeFuture;
import org.folio.okapi.common.HttpResponse;

public class OaiPmhClient {

  Vertx vertx;

  WebClient webClient;

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
    this.webClient = WebClient.create(vertx);
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

  static Future<Void> updateJob(Storage storage, SqlConnection connection, String id,
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

  static boolean addQueryParameterFromConfig(HttpRequest<Buffer> req, JsonObject config,
      String key) {
    String value = config.getString(key);
    if (value == null) {
      return false;
    }
    req.addQueryParam(key, value);
    return true;
  }

  static void addQueryParameterFromParams(HttpRequest<Buffer> req, JsonObject params) {
    if (params != null) {
      params.forEach(e -> {
        if (e.getValue() instanceof String value) {
          req.addQueryParam(e.getKey(), value);
        } else {
          throw new IllegalArgumentException("params " + e.getKey() + " value must be string");
        }
      });
    }
  }

  Future<Void> ingestRecord(Storage storage, OaiRecord oaiRecord,
      SourceId sourceId, JsonArray matchkeyconfigs) {
    return vertx.executeBlocking(p -> {
      try {
        JsonObject globalRecord = new JsonObject();
        globalRecord.put("localId", oaiRecord.getIdentifier());
        if (oaiRecord.getIsDeleted()) {
          globalRecord.put("delete", true);
        } else {
          JsonObject marc = XmlJsonUtil.convertMarcXmlToJson(oaiRecord.getMetadata());
          globalRecord.put("payload", new JsonObject().put("marc", marc));
        }
        storage.ingestGlobalRecord(vertx, sourceId, globalRecord, matchkeyconfigs).onComplete(p);
      } catch (Exception e) {
        log.error("{}", e.getMessage(), e);
        p.fail(e);
      }
    });
  }

  Future<Void> ingestRecords(Storage storage, SqlConnection connection, OaiParser oaiParser,
      JsonObject config) {
    SourceId sourceId = new SourceId(config.getString("sourceId"));
    return storage.getAvailableMatchConfigs(connection).compose(matchkeyconfigs -> {
      for (OaiRecord oaiRecord : oaiParser.getRecords()) {
        ingestRecord(storage, oaiRecord, sourceId, matchkeyconfigs);
      }
      return Future.succeededFuture();
    });
  }

  Future<Void> parseResponse(OaiParser oaiParser, String body) {
    return vertx.executeBlocking(p -> {
      try {
        oaiParser.clear();
        oaiParser.parseResponse(new ByteArrayInputStream(body.getBytes()));
      } catch (XMLStreamException e) {
        p.fail(e.getMessage());
        return;
      }
      p.complete();
    });
  }

  static void datestampToFrom(List<OaiRecord> oaiRecords, JsonObject config) {
    String newestDatestamp = null;
    for (OaiRecord oaiRecord : oaiRecords) {
      String datestamp = oaiRecord.getDatestamp();
      if (newestDatestamp == null || datestamp.compareTo(newestDatestamp) > 0) {
        newestDatestamp = datestamp;
      }
    }
    if (newestDatestamp != null) {
      config.put("from", Util.getNextOaiDate(newestDatestamp));
    }
  }

  void oaiHarvestLoop(Storage storage, SqlConnection connection, String id, JsonObject job) {
    JsonObject config = job.getJsonObject(CONFIG_LITERAL);

    log.info("oai client send request");
    HttpRequest<Buffer> req = webClient.getAbs(config.getString("url"))
        .putHeaders(getHttpHeaders(config));

    OaiParser oaiParser = new OaiParser();

    if (!addQueryParameterFromConfig(req, config, RESUMPTION_TOKEN_LITERAL)) {
      addQueryParameterFromConfig(req, config, "from");
      addQueryParameterFromConfig(req, config, "until");
      addQueryParameterFromConfig(req, config, "set");
      addQueryParameterFromConfig(req, config, "metadataPrefix");
    }
    addQueryParameterFromParams(req, config.getJsonObject("params"));
    req.addQueryParam("verb", "ListRecords")
        .send()
        .compose(res -> {
          log.info("oai client handle response");
          job.put(TOTAL_REQUESTS_LITERAL, job.getLong(TOTAL_REQUESTS_LITERAL) + 1);
          if (res.statusCode() != 200) {
            job.put(STATUS_LITERAL, IDLE_LITERAL);
            job.put("errors", "OAI server returned HTTP status "
                + res.statusCode() + "\n" + res.bodyAsString());
            return updateJob(storage, connection, id, job)
                .compose(x -> Future.failedFuture("stopping due to HTTP status error"));
          }
          return parseResponse(oaiParser, res.bodyAsString()).compose(d -> {
            List<OaiRecord> oaiRecords = oaiParser.getRecords();
            log.info("oai client ingest {} records", oaiRecords.size());
            job.put(TOTAL_RECORDS_LITERAL, job.getLong(TOTAL_RECORDS_LITERAL) + oaiRecords.size());
            datestampToFrom(oaiRecords, config);
            return ingestRecords(storage, connection, oaiParser, config)
                .compose(x -> {
                  String resumptionToken = oaiParser.getResumptionToken();
                  String oldResumptionToken = config.getString(RESUMPTION_TOKEN_LITERAL);
                  if (resumptionToken == null || resumptionToken.equals(oldResumptionToken)) {
                    config.remove(RESUMPTION_TOKEN_LITERAL);
                    job.put(STATUS_LITERAL, IDLE_LITERAL);
                    return updateJob(storage, connection, id, job)
                        .compose(e -> Future.failedFuture(
                            "stopping due to no resumptionToken or same resumptionToken"));
                  }
                  config.put(RESUMPTION_TOKEN_LITERAL, resumptionToken);
                  log.info("continuing with resumptionToken and records {}", job.encodePrettily());
                  return updateJob(storage, connection, id, job);
                });
          });
        })
        .onSuccess(x -> oaiHarvestLoop(storage, connection, id, job))
        .onFailure(e -> {
          log.error(e.getMessage(), e);
          connection.close();
        });
  }
}
