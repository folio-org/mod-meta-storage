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

  private static final String IDLE_ITERAL = "idle";

  private static final String RUNNING_LITERAL = "running";

  private static final String CONFIG_LITERAL = "config";

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
        job = new JsonObject().put(STATUS_LITERAL, IDLE_ITERAL);
      }
      job.put(CONFIG_LITERAL, row.getJsonObject(CONFIG_LITERAL));
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
          response.put("resultInfo", new JsonObject().put("totalRecords", ar.size()));
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
              if (IDLE_ITERAL.equals(status)) {
                HttpResponse.responseError(ctx, 400, "not running");
                return Future.succeededFuture();
              }
              job.put(STATUS_LITERAL, IDLE_ITERAL);
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

  Future<Void> ingestRecords(Storage storage, SqlConnection connection, OaiParser oaiParser,
      JsonObject config) {
    SourceId sourceId = new SourceId(config.getString("sourceId"));
    return storage.getAvailableMatchConfigs(connection).compose(matchkeyconfigs -> {
      List<Future<Void>> futures = new LinkedList<>();
      for (OaiRecord oaiRecord : oaiParser.getRecords()) {
        try {
          JsonObject globalRecord = new JsonObject();
          globalRecord.put("localId", oaiRecord.getIdentifier());
          if (oaiRecord.getIsDeleted()) {
            globalRecord.put("delete", true);
          } else {
            JsonObject marc = XmlJsonUtil.convertMarcXmlToJson(oaiRecord.getMetadata());
            globalRecord.put("payload", new JsonObject().put("marc", marc));
          }
          futures.add(storage.ingestGlobalRecord(vertx, sourceId, globalRecord, matchkeyconfigs));
        } catch (Exception e) {
          log.error("{}", e.getMessage(), e);
          return Future.failedFuture(e);
        }
      }
      return GenericCompositeFuture.all(futures).mapEmpty();
    });
  }

  void oaiHarvestLoop(Storage storage, SqlConnection connection, String id, JsonObject job) {
    JsonObject config = job.getJsonObject(CONFIG_LITERAL);

    HttpRequest<Buffer> req = webClient.getAbs(config.getString("url"))
        .putHeaders(getHttpHeaders(config));

    OaiParser oaiParser = new OaiParser();

    if (!addQueryParameterFromConfig(req, config, RESUMPTION_TOKEN_LITERAL)) {
      addQueryParameterFromConfig(req, config, "from");
      addQueryParameterFromConfig(req, config, "until");
      addQueryParameterFromConfig(req, config, "set");
      req.addQueryParam("marcDataPrefix", "marc21");
    }
    addQueryParameterFromConfig(req, config, "limit");
    req.addQueryParam("verb", "ListRecords")
        .send()
        .compose(res -> {
          if (res.statusCode() != 200) {
            job.put(STATUS_LITERAL, IDLE_ITERAL);
            job.put("errors", "OAI server returned HTTP status "
                + res.statusCode() + "\n" + res.bodyAsString());
            return updateJob(storage, connection, id, job)
                .compose(x -> Future.failedFuture("stopping due to HTTP status error"));
          }
          oaiParser.clear();
          try {
            oaiParser.applyResponse(new ByteArrayInputStream(res.bodyAsString().getBytes()));
          } catch (XMLStreamException e) {
            return Future.failedFuture(e.getMessage());
          }
          return ingestRecords(storage, connection, oaiParser, config)
              .compose(x -> {
                String resumptionToken = oaiParser.getResumptionToken();
                if (resumptionToken == null) {
                  config.remove(RESUMPTION_TOKEN_LITERAL);
                  job.put(STATUS_LITERAL, IDLE_ITERAL);
                  return updateJob(storage, connection, id, job)
                      .compose(e -> Future.failedFuture("stopping due to no resumptionToken"));
                }
                config.put(RESUMPTION_TOKEN_LITERAL, resumptionToken);
                log.info("continuing with resumptionToken");
                return updateJob(storage, connection, id, job);
              });
        })
        .onSuccess(x -> oaiHarvestLoop(storage, connection, id, job))
        .onFailure(e -> {
          log.error(e.getMessage(), e);
          connection.close();
        });
  }
}
