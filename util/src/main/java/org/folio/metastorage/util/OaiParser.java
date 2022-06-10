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

  private String datestamp;

  private final List<String> identifiers = new LinkedList<>();

  private final List<String> records = new LinkedList<>();

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
   * Get record identifiers.
   * @return list of identifiers
   */
  public List<String> getIdentifiers() {
    return identifiers;
  }

  /**
   * Get datestamp.
   * @return datestamp string (null if none provided)
   */
  public String getDateStamp() {
    return datestamp;
  }

  /**
   * Get record metadata content.
   * @return list of metadata with item being null for deleted metadata
   */
  public List<String> getMetadata() {
    return records;
  }

  /**
   * Clear identifiers and metadata records.
   */
  public void clear() {
    records.clear();
    identifiers.clear();
    resumptionToken = null;
    datestamp = null;
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
    int offset = 0;
    level = 0;
    String lastRecord = null;
    while (xmlStreamReader.hasNext()) {
      int event = next(xmlStreamReader);
      if (event == XMLStreamConstants.START_ELEMENT && xmlStreamReader.hasNext()) {
        String elem = xmlStreamReader.getLocalName();
        if (level == 3 && ("record".equals(elem) || "header".equals(elem))) {
          if (offset > 0) {
            records.add(lastRecord);
          }
          offset++;
          lastRecord = null;
        }
        if (level == 3 && "resumptionToken".equals(elem)) {
          event = next(xmlStreamReader);
          if (event == XMLStreamConstants.CHARACTERS) {
            resumptionToken = xmlStreamReader.getText();
          }
        }
        if (level == 4 && "metadata".equals(elem)) {
          lastRecord = XmlJsonUtil.getSubDocument(xmlStreamReader.next(), xmlStreamReader);
        }
        if (level == 5 && "datestamp".equals(elem)) {
          event = next(xmlStreamReader);
          if (event == XMLStreamConstants.CHARACTERS) {
            datestamp = xmlStreamReader.getText();
          }
        }
        if (level == 5 && "identifier".equals(elem)) {
          event = next(xmlStreamReader);
          if (event == XMLStreamConstants.CHARACTERS) {
            identifiers.add(xmlStreamReader.getText());
          }
        }
      }
    }
    if (offset > 0) {
      records.add(lastRecord);
    }
  }
}
