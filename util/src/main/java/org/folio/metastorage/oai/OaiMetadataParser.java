package org.folio.metastorage.oai;

import javax.xml.stream.XMLStreamReader;

public interface OaiMetadataParser<T> {

  void init();

  void handle(XMLStreamReader stream);

  T result();
}
