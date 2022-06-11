package org.folio.metastorage.util;

public class OaiRecord<T> {
  String datestamp;
  String identifier;
  boolean isDeleted;
  T metadata;

  public String getDatestamp() {
    return datestamp;
  }

  public String getIdentifier() {
    return identifier;
  }

  public boolean getIsDeleted() {
    return isDeleted;
  }

  public T getMetadata() {
    return metadata;
  }

}
