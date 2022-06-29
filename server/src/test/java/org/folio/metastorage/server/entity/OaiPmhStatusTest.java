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

  public void fromJson() {
    JsonObject o = new JsonObject()
        .put("status", "idle")
    OaiPmhStatus oaiPmhStatus = Json.decodeValue(o.encode(), OaiPmhStatus.class);
    assertThat(oaiPmhStatus.getStatus(), is("idle"));
  }
}
