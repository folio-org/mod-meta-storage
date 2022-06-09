package org.folio.metastorage.util;

import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

public class OaiParser {

  private XMLInputFactory factory;

  private String resumptionToken;

  private List<String> identifiers = new LinkedList<>();

  private List<String> records = new LinkedList<>();

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
  }

  /**
   * Parse OAI-PMH response from InputStream.
   * @param stream stream
   * @throws XMLStreamException stream exception
   */
  public void applyResponse(InputStream stream) throws XMLStreamException {
    XMLStreamReader xmlStreamReader = factory.createXMLStreamReader(stream);
    int offset = 0;
    int level = 0;
    String lastRecord = null;
    while (xmlStreamReader.hasNext()) {
      int event = xmlStreamReader.next();
      if (event == XMLStreamConstants.START_ELEMENT) {
        level++;
        String elem = xmlStreamReader.getLocalName();
        if (level == 3 && ("record".equals(elem) || "header".equals(elem))) {
          if (offset > 0) {
            records.add(lastRecord);
          }
          offset++;
          lastRecord = null;
        }
        if (level == 3 && "resumptionToken".equals(elem)) {
          event = xmlStreamReader.next();
          if (event == XMLStreamConstants.CHARACTERS) {
            resumptionToken = xmlStreamReader.getText();
          }
        }
        if (level == 4 && "metadata".equals(elem) && xmlStreamReader.hasNext()) {
          lastRecord = XmlJsonUtil.getSubDocument(xmlStreamReader.next(), xmlStreamReader);
        }
        if (level == 5 && "identifier".equals(elem) && xmlStreamReader.hasNext()) {
          event = xmlStreamReader.next();
          if (event == XMLStreamConstants.CHARACTERS) {
            identifiers.add(xmlStreamReader.getText());
          }
        }
      } else if (event == XMLStreamConstants.END_ELEMENT) {
        level--;
      }
    }
    if (offset > 0) {
      records.add(lastRecord);
    }
  }
}
