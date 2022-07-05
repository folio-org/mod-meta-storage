package org.folio.metastorage.server.entity;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import io.vertx.core.json.JsonObject;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import org.junit.Test;

public class OaiPmhStatusTest {

  @Test
  public void test() {
    OaiPmhStatus oaiPmhStatus = new OaiPmhStatus();
    assertThat(oaiPmhStatus.status, is(nullValue()));
    oaiPmhStatus.setStatus("running");
    assertThat(oaiPmhStatus.getStatus(), is("running"));
  }

  @Test
  public void mapFromMapTo() {
    JsonObject o = new JsonObject()
        .put("status", "idle");
    OaiPmhStatus oaiPmhStatus = o.mapTo(OaiPmhStatus.class);
    assertThat(oaiPmhStatus.getStatus(), is("idle"));
    JsonObject o2 = JsonObject.mapFrom(oaiPmhStatus);
    assertThat(o, is(o2));
  }

  @Test
  public void config() {
    OaiPmhStatus oaiPmhStatus = new OaiPmhStatus();
    JsonObject config = new JsonObject().put("id", "myidentifier");
    oaiPmhStatus.setConfig(config);
    JsonObject o = JsonObject.mapFrom(oaiPmhStatus);
    assertThat(o, is(new JsonObject()));
    JsonObject o1 = oaiPmhStatus.getJsonObject();
    assertThat(o1, is(new JsonObject().put("config", new JsonObject().put("id", "myidentifier"))));
  }

  @Test
  public void lastActiveTimestamp() {
    OaiPmhStatus oaiPmhStatus1 = new OaiPmhStatus();
    LocalDateTime now = LocalDateTime.now(ZoneOffset.UTC);
    oaiPmhStatus1.setLastActiveTimestampRaw(now);
    JsonObject o = JsonObject.mapFrom(oaiPmhStatus1);
    OaiPmhStatus oaiPmhStatus2 = o.mapTo(OaiPmhStatus.class);
    assertThat(oaiPmhStatus2.getLastActiveTimestampRaw(), is(now));
  }

  @Test
  public void lastStartedTimestamp() {
    OaiPmhStatus oaiPmhStatus1 = new OaiPmhStatus();
    LocalDateTime now = LocalDateTime.now();
    oaiPmhStatus1.setLastStartedTimestampRaw(now);
    JsonObject o = JsonObject.mapFrom(oaiPmhStatus1);
    OaiPmhStatus oaiPmhStatus2 = o.mapTo(OaiPmhStatus.class);
    assertThat(oaiPmhStatus2.getLastStartedTimestampRaw(), is(now));
  }

}
