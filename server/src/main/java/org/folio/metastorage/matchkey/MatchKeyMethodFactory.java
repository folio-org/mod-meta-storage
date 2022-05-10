package org.folio.metastorage.matchkey;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import java.util.HashMap;
import java.util.Map;
import org.folio.metastorage.matchkey.impl.MatchKeyJavaScript;
import org.folio.metastorage.matchkey.impl.MatchKeyJsonPath;

public class MatchKeyMethodFactory {

  private MatchKeyMethodFactory() { }

  private static Map<String, Map<JsonObject,MatchKeyMethod>> instances = new HashMap<>();

  /**
   * Get MatchKeyMethod instance from method.
   * @param method method name
   * @return method or NULL if not found
   */
  public static MatchKeyMethod get(String method) {
    if ("jsonpath".equals(method)) {
      return new MatchKeyJsonPath();
    } else if ("javascript".equals(method)) {
      return new MatchKeyJavaScript();
    }
    return null;
  }

  static Future<MatchKeyMethod> get(Vertx vertx, String method, JsonObject configuration) {
    synchronized (MatchKeyMethod.class) {
      Map<JsonObject, MatchKeyMethod> confMap = instances.get(method);
      if (confMap == null) {
        confMap = new HashMap<>();
        instances.put(method, confMap);
      } else {
        MatchKeyMethod matchKeyMethod = confMap.get(configuration);
        if (matchKeyMethod != null) {
          return Future.succeededFuture(matchKeyMethod);
        }
      }
      MatchKeyMethod m = get(method);
      if (m == null) {
        return Future.failedFuture("Unknown match key method " + method);
      }
      final Map<JsonObject,MatchKeyMethod> confMap1 = confMap;
      try {
        return m.configure(vertx, configuration).map(x -> {
          confMap1.put(configuration, m);
          return m;
        });
      } catch (Exception e) {
        return Future.failedFuture(e);
      }
    }
  }

}
