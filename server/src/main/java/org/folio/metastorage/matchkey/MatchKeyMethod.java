package org.folio.metastorage.matchkey;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import java.util.Collection;

public interface MatchKeyMethod {

  /**
   * Get MatchKeyMethod instance with configuration.
   * @param vertx Vert.x handle
   * @param method method name
   * @param configuration configuration
   * @return Async result MatchKeyMethod
   */
  static Future<MatchKeyMethod> get(Vertx vertx, String method, JsonObject configuration) {
    return MatchKeyMethodFactory.get(vertx, method, configuration);
  }

  Future<Void> configure(Vertx vertx, JsonObject configuration);

  /**
   * Generate match keys.
   * @param payload payload with marc and inventory XSLT result
   * @param keys resulting keys (unmodified if no keys were generated).
   */
  void getKeys(JsonObject payload, Collection<String> keys);
}
