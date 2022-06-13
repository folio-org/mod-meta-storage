package org.folio.metastorage.util;

import io.vertx.core.streams.ReadStream;
import java.util.function.Consumer;
import java.util.function.Supplier;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;

public class OaiParserStream<T> {

  int level;

  String elem;

  OaiRecord<T> lastRecord;

  StringBuilder cdata;

  String resumptionToken;

  int metadataLevel;

  void endElement() {
    if (elem != null) {
      String val = cdata.toString();
      switch (elem) {
        case "resumptionToken" -> resumptionToken = val;
        case "datestamp" -> lastRecord.datestamp = val;
        case "identifier" -> lastRecord.identifier = val;
        default -> { }
      }
      elem = null;
    }
    cdata.setLength(0);
  }

  /**
   * Get OAI-PMH resumption token.
   * @return token string
   */
  public String getResumptionToken() {
    return resumptionToken;
  }

  /**
   * Parse OAI response from stream.
   * @param stream XML parser stream
   * @param recordHandler handler that is called for each record
   * @param metadataParser metadata parser
   */
  public OaiParserStream(ReadStream<XMLStreamReader> stream, Consumer<OaiRecord<T>> recordHandler,
      OaiMetadataParser<T> metadataParser) {
    level = 0;
    elem = null;
    lastRecord = null;
    metadataLevel = 0;
    cdata = new StringBuilder();
    stream.handler(xmlStreamReader -> {
      try {
        int event = xmlStreamReader.getEventType();
        if (event == XMLStreamConstants.END_DOCUMENT) {
          if (lastRecord != null) {
            recordHandler.accept(lastRecord);
          }
        }
        if (event == XMLStreamConstants.START_ELEMENT) {
          level++;
          if (metadataLevel != 0 && level > metadataLevel) {
            metadataParser.handle(xmlStreamReader);
          } else {
            endElement();
            elem = xmlStreamReader.getLocalName();
            if (level == 3 && ("record".equals(elem) || "header".equals(elem))) {
              if (lastRecord != null) {
                recordHandler.accept(lastRecord);
              }
              lastRecord = new OaiRecord<>();
              metadataParser.init();
            }
            if ("header".equals(elem)) {
              for (int i = 0; i < xmlStreamReader.getAttributeCount(); i++) {
                if ("status".equals(xmlStreamReader.getAttributeLocalName(i))
                    && "deleted".equals(xmlStreamReader.getAttributeValue(i))) {
                  lastRecord.deleted = true;
                }
              }
            }
            if ("metadata".equals(elem)) {
              metadataLevel = level;
            }
          }
        } else if (event == XMLStreamConstants.END_ELEMENT) {
          level--;
          if (metadataLevel != 0) {
            if (level > metadataLevel) {
              metadataParser.handle(xmlStreamReader);
            } else {
              lastRecord.metadata = metadataParser.result();
              metadataLevel = 0;
            }
          } else {
            endElement();
          }
        } else if (metadataLevel != 0 && level > metadataLevel) {
          metadataParser.handle(xmlStreamReader);
        } else if (event == XMLStreamConstants.CHARACTERS) {
          cdata.append(xmlStreamReader.getText());
        }
      } catch (Exception e) {
        System.out.println(e.getMessage());
      }
    });
  }
}
