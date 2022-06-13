package org.folio.metastorage.util;

public class OaiRecord<T> {
  String datestamp;
  String identifier;
  boolean deleted;
  T metadata;

  public String getDatestamp() {
    return datestamp;
  }

  public String getIdentifier() {
    return identifier;
  }

  public boolean isDeleted() {
    return deleted;
  }

  public T getMetadata() {
    return metadata;
  }

}
