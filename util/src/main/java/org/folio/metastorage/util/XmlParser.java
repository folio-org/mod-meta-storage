package org.folio.metastorage.util;

import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.ReadStream;
import javax.xml.stream.XMLStreamReader;

public interface XmlParser extends ReadStream<XMLStreamReader>, Handler<Buffer> {
  static XmlParser newParser(ReadStream<Buffer> stream) {
    return new XmlParserImpl(stream);
  }

  void end();
}
