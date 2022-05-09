package org.folio.metastorage.matchkey.impl;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Collection;
import org.folio.metastorage.matchkey.MatchKeyMethod;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;

public class MatchKeyJavaScript implements MatchKeyMethod {

  Value getKeysFunction;

  org.graalvm.polyglot.Context context;

  @Override
  public Future<Void> configure(JsonObject configuration) {
    String filename = configuration.getString("filename");
    String script = configuration.getString("script");
    if (script != null) {
      context = org.graalvm.polyglot.Context.create("js");
      getKeysFunction = context.eval("js", script);
    } else if (filename != null) {
      File file = new File(filename);
      context = org.graalvm.polyglot.Context.create("js");
      Source source;
      try {
        source = Source.newBuilder("js", file).build();
      } catch (IOException e) {
        return Future.failedFuture(e);
      }
      getKeysFunction = context.eval(source);
    } else {
      return Future.failedFuture("javascript: filename or script must be given");
    }
    return Future.succeededFuture();
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
