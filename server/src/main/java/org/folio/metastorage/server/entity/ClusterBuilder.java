package org.folio.metastorage.server.entity;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.UUID;

public class ClusterBuilder {
  
  private JsonObject clusterJson = new JsonObject();
   
  public ClusterBuilder(UUID clusterId) {
    clusterJson.put("clusterId", clusterId.toString());
  }

  public ClusterBuilder datestamp(LocalDateTime datestamp) {
    clusterJson.put("datestamp", datestamp.atZone(ZoneOffset.UTC).toString());
    return this;
  }
  
  /**
   * Add records from a RowSet.
   * @param rows row set
   * @return this
   */
  public ClusterBuilder records(RowSet<Row> rows) {
    JsonArray records = new JsonArray();
    rows.forEach(row -> records.add(encodeRecord(row)));
    clusterJson.put("records", records);
    return this;
  }

  /**
   * Add matchValues from a RowSet.
   * @param rows row set
   * @return this
   */
  public ClusterBuilder matchValues(RowSet<Row> rows) {
    JsonArray matchValues = new JsonArray();
    rows.forEach(row -> matchValues.add(row.getString("match_value")));
    clusterJson.put("matchValues", matchValues);
    return this;
  }

  public JsonObject build() {
    return clusterJson;
  }

  /**
   * Encodes a single global record row as JSON.
   * @param row global record row
   * @return JSON encoding
   */
  public static JsonObject encodeRecord(Row row) {
    return new JsonObject()
      .put("globalId", row.getUUID("id"))
      .put("localId", row.getString("local_id"))
      .put("sourceId", row.getString("source_id"))
      .put("payload", row.getJsonObject("payload"));
  }
}
