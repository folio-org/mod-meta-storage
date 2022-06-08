package org.folio.metastorage.server;

import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.validation.RequestParameters;
import io.vertx.ext.web.validation.ValidationHandler;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowIterator;
import io.vertx.sqlclient.Tuple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.okapi.common.HttpResponse;

public final class OaiPmhClient {
  private static final Logger log = LogManager.getLogger(OaiPmhClient.class);

  private OaiPmhClient() {
    throw new UnsupportedOperationException("OaiPmhClient");
  }

  /**
   * Create OAI-PMH client.
   *
   * @param ctx routing context
   * @return async result
   */
  public static Future<Void> post(RoutingContext ctx) {
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

  static Future<JsonObject> getConfig(Storage storage, String id) {
    return storage.getPool().preparedQuery("SELECT * FROM " + storage.getOaiPmhClientTable()
            + " WHERE id = $1")
        .execute(Tuple.of(id))
        .map(rowSet -> {
          RowIterator<Row> iterator = rowSet.iterator();
          if (!iterator.hasNext()) {
            return null;
          }
          Row row = iterator.next();
          JsonObject job = row.getJsonObject("job");
          if (job == null) {
            job = new JsonObject().put("status", "idle");
          }
          return new JsonObject()
              .put("config", row.getJsonObject("config"))
              .put("job", job);
        });
  }

  /**
   * Get OAI-PMH client.
   * @param ctx routing context
   * @return async result
   */
  public static Future<Void> get(RoutingContext ctx) {
    Storage storage = new Storage(ctx);
    RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
    String id = Util.getParameterString(params.pathParameter("id"));
    return getConfig(storage, id).map(config -> {
      if (config == null) {
        HttpResponse.responseError(ctx, 404, id);
        return null;
      }
      JsonObject conf = config.getJsonObject("config").put("id", id);
      HttpResponse.responseJson(ctx, 200).end(conf.encode());
      return null;
    });
  }

  /**
   * Get all OAI-PMH clients.
   * @param ctx routing context
   * @return async result
   */
  public static Future<Void> getCollection(RoutingContext ctx) {
    Storage storage = new Storage(ctx);
    return storage.getPool().query("SELECT id,config FROM " + storage.getOaiPmhClientTable())
        .execute()
        .map(rowSet -> {
          JsonArray ar = new JsonArray();
          rowSet.forEach(x -> {
            JsonObject config = x.getJsonObject("config");
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
  public static Future<Void> delete(RoutingContext ctx) {
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
  public static Future<Void> put(RoutingContext ctx) {
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

  static Future<Void> updateJob(Storage storage, String id, JsonObject job) {
    return storage.getPool().preparedQuery("UPDATE " + storage.getOaiPmhClientTable()
        + " SET job = $2 WHERE id = $1")
        .execute(Tuple.of(id, job))
        .mapEmpty();
  }

  /**
   * Start OAI PMH client job.
   * @param ctx routing context
   * @return async result
   */
  public static Future<Void> start(RoutingContext ctx) {
    Storage storage = new Storage(ctx);
    RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
    String id = Util.getParameterString(params.pathParameter("id"));

    return getConfig(storage, id)
        .compose(config -> {
          if (config == null) {
            HttpResponse.responseError(ctx, 404, id);
            return Future.succeededFuture();
          }
          JsonObject job = config.getJsonObject("job");
          String status = job.getString("status");
          if ("running".equals(status)) {
            HttpResponse.responseError(ctx, 400, "already running");
            return Future.succeededFuture();
          }
          job.put("status", "running");
          return updateJob(storage, id, job)
              .map(x -> {
                ctx.response().setStatusCode(204).end();
                return null;
              });
        })
        .mapEmpty();
  }

  /**
   * Stop OAI PMH client job.
   * @param ctx routing context
   * @return async result
   */
  public static Future<Void> stop(RoutingContext ctx) {
    Storage storage = new Storage(ctx);
    RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
    String id = Util.getParameterString(params.pathParameter("id"));

    return getConfig(storage, id).compose(config -> {
      if (config == null) {
        HttpResponse.responseError(ctx, 404, id);
        return Future.succeededFuture();
      }
      JsonObject job = config.getJsonObject("job");
      String status = job.getString("status");
      if ("idle".equals(status)) {
        HttpResponse.responseError(ctx, 400, "not running");
        return Future.succeededFuture();
      }
      job.put("status", "idle");
      return updateJob(storage, id, job)
          .map(x -> {
            ctx.response().setStatusCode(204).end();
            return null;
          });
    });
  }

  /**
   * Get OAI PMH client status.
   * @param ctx routing context
   * @return async result
   */
  public static Future<Void> status(RoutingContext ctx) {
    Storage storage = new Storage(ctx);
    RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
    String id = Util.getParameterString(params.pathParameter("id"));

    return getConfig(storage, id).map(config -> {
      if (config == null) {
        HttpResponse.responseError(ctx, 404, id);
        return null;
      }
      JsonObject job = config.getJsonObject("job");
      HttpResponse.responseJson(ctx, 200).end(job.encode());
      return null;
    });
  }

}
