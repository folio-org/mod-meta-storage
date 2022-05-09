package org.folio.metastorage.matchkey;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import java.util.Collection;
import org.folio.metastorage.matchkey.impl.MatchKeyJavaScript;
import org.folio.metastorage.matchkey.impl.MatchKeyJsonPath;

public interface MatchKeyMethod {

  /**
   * Get MatchKeyMethod instance with configuration.
   * @param method method name
   * @param configuration configuration
   * @return Async result MatchKeyMethod
   */
  static Future<MatchKeyMethod> get(String method, JsonObject configuration) {
    MatchKeyMethod m = get(method);
    if (m == null) {
      return Future.failedFuture("Unknown match key method " + method);
    }
    try {
      return m.configure(configuration).map(m);
    } catch (Exception e) {
      return Future.failedFuture(e);
    }
  }

  /**
   * Get MatchKeyMethod instance from method.
   * @param method method name
   * @return method or NULL if not found
   */
  static MatchKeyMethod get(String method) {
    if ("jsonpath".equals(method)) {
      return new MatchKeyJsonPath();
    } else if ("javascript".equals(method)) {
      return new MatchKeyJavaScript();
    }
    return null;
  }

  Future<Void> configure(JsonObject configuration);

  /**
   * Generate match keys.
   * @param payload payload with marc and inventory XSLT result
   * @param keys resulting keys (unmodified if no keys were generated).
   */
  void getKeys(JsonObject payload, Collection<String> keys);
}
