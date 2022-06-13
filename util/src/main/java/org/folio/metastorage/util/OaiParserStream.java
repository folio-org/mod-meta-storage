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
    if (elem == null) {
      cdata.setLength(0);
      return;
    }
    String val = cdata.toString();
    switch (elem) {
      case "resumptionToken":
        resumptionToken = val;
        break;
      case "datestamp":
        lastRecord.datestamp = val;
        break;
      case "identifier":
        lastRecord.identifier = val;
        break;
      default:
    }
    cdata.setLength(0);
    elem = null;
  }

  private Consumer<XMLStreamReader> metadataHandler;

  /**
   * Set handler that is called for XML events inside "metadata" element.
   * @param handler method for handling XML stream event
   */
  public void setMetadataHandler(Consumer<XMLStreamReader> handler) {
    metadataHandler = handler;
  }

  private Supplier<T> metadataCreate;

  /**
   * Set handler that constructs metadata of type T.
   * @param s supplier/handle
   */
  public void setMetadataCreate(Supplier<T> s) {
    metadataCreate = s;
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
   */
  public OaiParserStream(ReadStream<XMLStreamReader> stream, Consumer<OaiRecord<T>> recordHandler) {
    level = 0;
    elem = null;
    lastRecord = null;
    metadataLevel = 0;
    cdata = new StringBuilder();
    stream.handler(xmlStreamReader -> {
      int event = xmlStreamReader.getEventType();
      if (event == XMLStreamConstants.START_ELEMENT) {
        level++;
        if (metadataLevel != 0 && level > metadataLevel) {
          metadataHandler.accept(xmlStreamReader);
        } else {
          endElement();
          elem = xmlStreamReader.getLocalName();
          if (level == 3 && ("record".equals(elem) || "header".equals(elem))) {
            if (lastRecord != null) {
              recordHandler.accept(lastRecord);
            }
            lastRecord = new OaiRecord<>();
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
        if (level < metadataLevel) {
          lastRecord.metadata = metadataCreate.get();
          metadataLevel = 0;
        }
        endElement();
      } else if (event == XMLStreamConstants.CHARACTERS) {
        cdata.append(xmlStreamReader.getText());
      }
    });
    stream.endHandler(end -> {
      if (lastRecord != null) {
        recordHandler.accept(lastRecord);
      }
    });
  }
}
