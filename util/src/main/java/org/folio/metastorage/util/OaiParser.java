package org.folio.metastorage.util;

import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

public class OaiParser {

  private final XMLInputFactory factory;

  private String resumptionToken;

  Function<XMLStreamReader, String> parseMetadata = x -> {
    try {
      return XmlJsonUtil.getSubDocument(x.next(), x);
    } catch (XMLStreamException e) {
      throw new RuntimeException(e);
    }
  };

  private int level;

  public OaiParser() {
    factory = XMLInputFactory.newInstance();
    factory.setProperty(XMLInputFactory.SUPPORT_DTD, false);
  }

  /**
   * Set parse metadata handler.
   * @param parseMetadata handler converts XML stream of metadata content to string.
   */
  public void setParseMetadata(Function<XMLStreamReader,String> parseMetadata) {
    this.parseMetadata = parseMetadata;
  }

  /**
   * Get resumption token.
   * @return token string or null if none
   */
  public String getResumptionToken() {
    return resumptionToken;
  }

  /**
   * Clear identifiers and metadata records.
   */
  public void clear() {
    resumptionToken = null;
  }

  int next(XMLStreamReader xmlStreamReader) throws XMLStreamException {
    int event = xmlStreamReader.next();
    if (event == XMLStreamConstants.START_ELEMENT) {
      level++;
    } else if (event == XMLStreamConstants.END_ELEMENT) {
      level--;
    }
    return event;
  }

  void header(XMLStreamReader xmlStreamReader, OaiRecord lastRecord) {
    for (int i = 0; i < xmlStreamReader.getAttributeCount(); i++) {
      if ("status".equals(xmlStreamReader.getAttributeLocalName(i))
          && "deleted".equals(xmlStreamReader.getAttributeValue(i))) {
        lastRecord.isDeleted = true;
      }
    }
  }

  void parseResumptionToken(XMLStreamReader xmlStreamReader) throws XMLStreamException {
    int event = next(xmlStreamReader);
    if (event == XMLStreamConstants.CHARACTERS) {
      resumptionToken = xmlStreamReader.getText();
    }
  }

  void parseDatestamp(XMLStreamReader xmlStreamReader, OaiRecord oaiRecord)
      throws XMLStreamException {
    int event = next(xmlStreamReader);
    if (event == XMLStreamConstants.CHARACTERS) {
      oaiRecord.datestamp = xmlStreamReader.getText();
    }
  }

  void parseIdentifier(XMLStreamReader xmlStreamReader, OaiRecord oaiRecord)
      throws XMLStreamException {
    int event = next(xmlStreamReader);
    if (event == XMLStreamConstants.CHARACTERS) {
      oaiRecord.identifier = (xmlStreamReader.getText());
    }
  }

  /**
   * Parse OAI-PMH response from InputStream and invoke for each record.
   * @param stream stream
   * @param recordHandler record consumer
   * @throws XMLStreamException stream exception
   */

  public void parseResponse(InputStream stream, Consumer<OaiRecord> recordHandler)
      throws XMLStreamException {
    XMLStreamReader xmlStreamReader = factory.createXMLStreamReader(stream);
    parseResponse(xmlStreamReader, recordHandler);
  }

  /**
   * Parse OAI-PMH response from xmlStreamReader with custom metadata parser.
   * @param xmlStreamReader reader that is used for reading MARC-XML
   * @param recordHandler called for each record
   * @throws XMLStreamException stream exception
   */
  public void parseResponse(XMLStreamReader xmlStreamReader, Consumer<OaiRecord> recordHandler)
      throws XMLStreamException {
    level = 0;
    OaiRecord lastRecord = null;
    while (xmlStreamReader.hasNext()) {
      int event = next(xmlStreamReader);
      if (event == XMLStreamConstants.START_ELEMENT && xmlStreamReader.hasNext()) {
        String elem = xmlStreamReader.getLocalName();
        if (level == 3 && ("record".equals(elem) || "header".equals(elem))) {
          if (lastRecord != null) {
            recordHandler.accept(lastRecord);
          }
          lastRecord = new OaiRecord();
        }
        if ("header".equals(elem)) {
          header(xmlStreamReader, lastRecord);
        } else if ("resumptionToken".equals(elem)) {
          parseResumptionToken(xmlStreamReader);
        } else if ("metadata".equals(elem)) {
          lastRecord.metadata = parseMetadata.apply(xmlStreamReader);
        } else if ("datestamp".equals(elem)) {
          parseDatestamp(xmlStreamReader, lastRecord);
        } else if ("identifier".equals(elem)) {
          parseIdentifier(xmlStreamReader, lastRecord);
        }
      }
    }
    if (lastRecord != null) {
      recordHandler.accept(lastRecord);
    }
  }

}
