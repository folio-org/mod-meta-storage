package org.folio.metastorage.server.entity;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import org.junit.Test;

public class OaiPmhStatusTest {

  @Test
  public void test() {
    OaiPmhStatus oaiPmhStatus = new OaiPmhStatus();
    assertThat(oaiPmhStatus.status, is(nullValue()));
    oaiPmhStatus.setStatus("running");
    assertThat(oaiPmhStatus.getStatus(), is("running"));
  }

  public void mapFromMapTo() {
    JsonObject o = new JsonObject()
        .put("status", "idle");
    OaiPmhStatus oaiPmhStatus = o.mapTo(OaiPmhStatus.class);
    assertThat(oaiPmhStatus.getStatus(), is("idle"));
    JsonObject o2 = JsonObject.mapFrom(oaiPmhStatus);
    assertThat(o, is(o2));
  }
}
