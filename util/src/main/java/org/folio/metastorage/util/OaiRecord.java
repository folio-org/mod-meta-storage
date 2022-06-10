package org.folio.metastorage.util;

public class OaiRecord {
  String datestamp;
  String identifier;
  boolean isDeleted;
  String metadata;

  public String getDatestamp() {
    return datestamp;
  }

  public String getIdentifier() {
    return identifier;
  }

  public boolean getIsDeleted() {
    return isDeleted;
  }

  public String getMetadata() {
    return metadata;
  }

}
