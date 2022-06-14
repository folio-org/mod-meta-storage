package org.folio.metastorage.util;

import io.vertx.core.Handler;
import io.vertx.core.streams.ReadStream;
import java.util.function.Consumer;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;

public class OaiParserStream<T> {

  private Handler<Throwable> exceptionHandler;

  int level;

  String elem;

  OaiRecord<T> lastRecord;

  final StringBuilder cdata = new StringBuilder();

  String resumptionToken;

  int metadataLevel;

  Consumer<String> handleText;

  int textLevel;

  void setHandleText(Consumer<String> handle) {
    cdata.setLength(0);
    textLevel = level;
    handleText = handle;
  }

  void checkHandleText() {
    if (level >= textLevel) {
      return;
    }
    if (handleText != null) {
      handleText.accept(cdata.toString());
    }
    cdata.setLength(0);
    handleText = null;
  }

  /**
   * Get OAI-PMH resumption token.
   * @return token string
   */
  public String getResumptionToken() {
    return resumptionToken;
  }

  public OaiParserStream<T> exceptionHandler(Handler<Throwable> handler) {
    exceptionHandler = handler;
    return this;
  }

  /**
   * Parse OAI response from stream.
   * @param stream XML parser stream
   * @param recordHandler handler that is called for each record
   * @param metadataParser metadata parser
   */
  public OaiParserStream(ReadStream<XMLStreamReader> stream, Consumer<OaiRecord<T>> recordHandler,
      OaiMetadataParser<T> metadataParser) {
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
            } else if ("metadata".equals(elem)) {
              metadataLevel = level;
            } else if ("resumptionToken".equals(elem)) {
              setHandleText(text -> resumptionToken = text);
            } else if ("datestamp".equals(elem)) {
              setHandleText(text -> lastRecord.datestamp = text);
            } else if ("identifier".equals(elem)) {
              setHandleText(text -> lastRecord.identifier = text);
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
            checkHandleText();
          }
        } else if (metadataLevel != 0 && level > metadataLevel) {
          metadataParser.handle(xmlStreamReader);
        } else if (event == XMLStreamConstants.CHARACTERS) {
          cdata.append(xmlStreamReader.getText());
        }
      } catch (Exception e) {
        exceptionHandler.handle(e);
      }
    });
    stream.exceptionHandler(e -> exceptionHandler.handle(e));
  }
}
