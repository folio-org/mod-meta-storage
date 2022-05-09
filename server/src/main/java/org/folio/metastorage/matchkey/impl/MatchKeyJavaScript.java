package org.folio.metastorage.matchkey.impl;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import java.util.Collection;
import org.folio.metastorage.matchkey.MatchKeyMethod;
import org.graalvm.polyglot.Value;

public class MatchKeyJavaScript implements MatchKeyMethod {

  Value getKeysFunction;

  org.graalvm.polyglot.Context context;

  @Override
  public Future<Void> configure(JsonObject configuration) {
    String filename = configuration.getString("filename");
    String script = configuration.getString("script");
    Future<String> future;
    if (script != null) {
      future = Future.succeededFuture(script);
    } else if (filename != null) {
      Vertx vertx = Vertx.currentContext().owner();
      future = vertx.fileSystem().readFile(filename).map(Buffer::toString);
    } else {
      return Future.failedFuture("javascript: filename or script must be given");
    }
    return future.map(content -> {
      context = org.graalvm.polyglot.Context.create("js");
      getKeysFunction = context.eval("js", content);
      return null;
    });
  }

  private void addValue(Collection<String> keys, Value value) {
    if (value.isNumber()) {
      keys.add(Long.toString(value.asLong()));
    } else if (value.isString()) {
      keys.add(value.asString());
    }
  }

  @Override
  public void getKeys(JsonObject payload, Collection<String> keys) {
    Value value = getKeysFunction.execute(payload.encode());
    if (value.hasArrayElements()) {
      for (int i = 0; i < value.getArraySize(); i++) {
        Value memberValue = value.getArrayElement(i);
        addValue(keys, memberValue);
      }
    } else {
      addValue(keys, value);
    }
  }
}
