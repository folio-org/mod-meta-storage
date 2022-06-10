package org.folio.metastorage.util;

import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;
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

  /**
   * Parse OAI-PMH response from InputStream.
   * @param stream stream
   * @throws XMLStreamException stream exception
   */
  public void applyResponse(InputStream stream) throws XMLStreamException {
    XMLStreamReader xmlStreamReader = factory.createXMLStreamReader(stream);
    level = 0;
    OaiRecord lastRecord = null;
    while (xmlStreamReader.hasNext()) {
      int event = next(xmlStreamReader);
      if (event == XMLStreamConstants.START_ELEMENT && xmlStreamReader.hasNext()) {
        String elem = xmlStreamReader.getLocalName();
        if (level == 3 && ("record".equals(elem) || "header".equals(elem))) {
          if (lastRecord != null) {
            records.add(lastRecord);
          }
          lastRecord = new OaiRecord();
        }
        if ("header".equals(elem) && level <= 4) {
          for (int i = 0; i < xmlStreamReader.getAttributeCount(); i++) {
            if ("status".equals(xmlStreamReader.getAttributeLocalName(i))
                && "deleted".equals(xmlStreamReader.getAttributeValue(i))) {
              lastRecord.isDeleted = true;
            }
          }
        }
        if (level == 3 && "resumptionToken".equals(elem)) {
          event = next(xmlStreamReader);
          if (event == XMLStreamConstants.CHARACTERS) {
            resumptionToken = xmlStreamReader.getText();
          }
        }
        if (level == 4 && "metadata".equals(elem)) {
          lastRecord.metadata = XmlJsonUtil.getSubDocument(xmlStreamReader.next(), xmlStreamReader);
        }
        if (level == 5 && "datestamp".equals(elem)) {
          event = next(xmlStreamReader);
          if (event == XMLStreamConstants.CHARACTERS) {
            lastRecord.datestamp = xmlStreamReader.getText();
          }
        }
        if (level == 5 && "identifier".equals(elem)) {
          event = next(xmlStreamReader);
          if (event == XMLStreamConstants.CHARACTERS) {
            lastRecord.identifier = (xmlStreamReader.getText());
          }
        }
      }
    }
    if (lastRecord != null) {
      records.add(lastRecord);
    }
  }
}
