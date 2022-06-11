package org.folio.metastorage.util;

import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Consumer;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

public class OaiParser {

  private final XMLInputFactory factory;

  private String resumptionToken;

  private final List<OaiRecord> records = new LinkedList<>();

  private int level;

  public OaiParser() {
    factory = XMLInputFactory.newInstance();
    factory.setProperty(XMLInputFactory.SUPPORT_DTD, false);
  }

  /**
   * Get resumption token.
   * @return token string or null if none
   */
  public String getResumptionToken() {
    return resumptionToken;
  }

  /**
   * Get records.
   * @return list of records
   */
  public List<OaiRecord> getRecords() {
    return records;
  }

  /**
   * Clear identifiers and metadata records.
   */
  public void clear() {
    records.clear();
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
   * Parse OAI-PMH response from InputStream and save list.
   * @param stream stream
   * @throws XMLStreamException stream exception
   */
  public void parseResponse(InputStream stream) throws XMLStreamException {
    parseResponse(stream, records::add);
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
          lastRecord.metadata = XmlJsonUtil.getSubDocument(xmlStreamReader.next(), xmlStreamReader);
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
