package org.folio.metastorage.util;

import static org.folio.metastorage.util.Constants.COLLECTION_LABEL;
import static org.folio.metastorage.util.Constants.FIELDS_LABEL;
import static org.folio.metastorage.util.Constants.RECORD_LABEL;
import static org.folio.metastorage.util.Constants.SUBFIELDS_LABEL;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Iterator;
import java.util.List;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.transform.Source;
import javax.xml.transform.Templates;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public final class XmlJsonUtil {
  private static final Logger LOGGER = LogManager.getLogger(XmlJsonUtil.class);

  private XmlJsonUtil() {
    throw new UnsupportedOperationException("XmlJsonUtil");
  }

  static String getXmlStreamerEventInfo(int event, XMLStreamReader xmlStreamReader) {
    switch (event) {
      case XMLStreamConstants.END_ELEMENT:
        return "END " + xmlStreamReader.getLocalName();
      case XMLStreamConstants.START_ELEMENT:
        return "START " + xmlStreamReader.getLocalName();
      case XMLStreamConstants.CHARACTERS:
        return "CHARACTERS '" + xmlStreamReader.getText() + "'";
      default:
        return String.valueOf(event);
    }
  }

  private static int next(XMLStreamReader xmlStreamReader) throws XMLStreamException {
    int event = xmlStreamReader.next();
    LOGGER.debug("next {}", () -> getXmlStreamerEventInfo(event, xmlStreamReader));
    return event;
  }

  static JsonArray xmlToJsonArray(int depth, XMLStreamReader xmlStreamReader, String skip)
      throws XMLStreamException {
    JsonArray ar = new JsonArray();
    while (true) {
      int event = next(xmlStreamReader);
      if (event == XMLStreamConstants.START_ELEMENT) {
        JsonObject arrayObject = new JsonObject();
        xmlToJsonObject(depth + 1, xmlStreamReader, skip, event, arrayObject);
        Iterator<String> iterator = arrayObject.fieldNames().iterator();
        // take content of "i" element
        if (iterator.hasNext()) {
          ar.add(arrayObject.getValue(iterator.next()));
        }
      } else if (event != XMLStreamConstants.CHARACTERS) {
        break;
      }
    }
    return ar;
  }

  static void xmlToJsonSkip(XMLStreamReader xmlStreamReader, int event) throws XMLStreamException {
    int level = 0;
    while (true) {
      if (event == XMLStreamConstants.END_ELEMENT) {
        level--;
        if (level == 0) {
          break;
        }
      } else if (event == XMLStreamConstants.START_ELEMENT) {
        level++;
      }
      event = next(xmlStreamReader);
    }
  }

  static Object xmlToJsonObject(int depth, XMLStreamReader xmlStreamReader, String skip, int event,
      JsonObject arrayObject) throws XMLStreamException {
    StringBuilder text = null;
    JsonObject o = arrayObject;
    JsonArray ar = null;
    while (true) {
      if (event == XMLStreamConstants.START_ELEMENT) {
        String localName = xmlStreamReader.getLocalName();
        if (arrayObject == null && "arr".equals(localName)) {
          ar = xmlToJsonArray(depth, xmlStreamReader, skip);
        } else if (skip.equals(localName)) {
          xmlToJsonSkip(xmlStreamReader, event);
        } else {
          event = next(xmlStreamReader);
          if (o == null) {
            o = new JsonObject();
          }
          o.put(localName, xmlToJsonObject(depth + 1, xmlStreamReader, skip, event, null));
          if (!xmlStreamReader.hasNext() || arrayObject != null) {
            return o;
          }
        }
      } else if (arrayObject == null && event == XMLStreamConstants.CHARACTERS) {
        if (text == null) {
          text = new StringBuilder();
        }
        text.append(xmlStreamReader.getText());
      } else {
        break;
      }
      event = next(xmlStreamReader);
    }
    if (ar != null) {
      return ar;
    } else if (o != null) {
      return o;
    } else if (text != null) {
      return text.toString();
    } else {
      return null;
    }
  }

  /**
   * Convert "inventory" XML to JSON.
   * @param xml inventory XML
   * @return json object without original record
   * @throws XMLStreamException bad XML
   */
  public static JsonObject inventoryXmlToJson(String xml) throws XMLStreamException {
    InputStream stream = new ByteArrayInputStream(xml.getBytes());
    XMLInputFactory factory = XMLInputFactory.newInstance();
    factory.setProperty(XMLInputFactory.SUPPORT_DTD, false);
    XMLStreamReader xmlStreamReader = factory.createXMLStreamReader(stream);
    Object o = xmlToJsonObject(0, xmlStreamReader, "original", next(xmlStreamReader), null);
    if (o instanceof JsonObject jsonObject) {
      return jsonObject;
    }
    throw new IllegalArgumentException("xmlToJsonObject not returning JsonObject");
  }

  /**
   * Encode encode XML string.
   * @param s string
   * @return encoded string
   */
  public static String encodeXmlText(String s) {
    StringBuilder res = new StringBuilder();
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      switch (c) {
        case '<':
          res.append("&lt;");
          break;
        case '>':
          res.append("&gt;");
          break;
        case '&':
          res.append("&amp;");
          break;
        case '\"':
          res.append("&quot;");
          break;
        case '\'':
          res.append("&apos;");
          break;
        default:
          res.append(c);
      }
    }
    return res.toString();
  }

  /**
   * Returns XML serialized document for node in XML.
   *
   * <p>This method does not care about namespaces. Only elements (local names), attributes
   * and text is dealt with.
   *
   * @param event event type for node that begins the sub document
   * @param reader XML stream reader
   * @return XML document string; null if no more documents in stream
   * @throws XMLStreamException if there's an exception for the XML stream
   */
  public static String getSubDocument(int event, XMLStreamReader reader)
      throws XMLStreamException {
    int level = 0;
    Buffer buffer = Buffer.buffer();
    for (; reader.hasNext(); event = reader.next()) {
      switch (event) {
        case XMLStreamConstants.START_ELEMENT:
          level++;
          buffer.appendString("<").appendString(reader.getLocalName());
          if (level == 1) {
            String uri = reader.getNamespaceURI();
            if (uri != null && uri.length() > 0) {
              buffer
                  .appendString(" xmlns=\"")
                  .appendString(uri)
                  .appendString("\"");
            }
          }
          for (int i = 0; i < reader.getAttributeCount(); i++) {
            buffer
                .appendString(" ")
                .appendString(reader.getAttributeLocalName(i))
                .appendString("=\"")
                .appendString(encodeXmlText(reader.getAttributeValue(i)))
                .appendString("\"");
          }
          buffer.appendString(">");
          break;
        case XMLStreamConstants.END_ELEMENT:
          level--;
          buffer.appendString("</").appendString(reader.getLocalName()).appendString(">");
          if (level == 0) {
            return buffer.toString();
          }
          break;
        case XMLStreamConstants.CHARACTERS:
          buffer.appendString(encodeXmlText(reader.getText()));
          break;
        default:
      }
    }
    return null;
  }

  static JsonObject createIngestRecord(JsonObject marcPayload, JsonObject stylesheetResult) {
    if (stylesheetResult.containsKey(COLLECTION_LABEL)) {
      stylesheetResult = stylesheetResult.getJsonObject(COLLECTION_LABEL);
    }
    JsonObject inventoryPayload = stylesheetResult.getJsonObject(RECORD_LABEL);
    if (inventoryPayload == null) {
      throw new IllegalArgumentException("inventory xml: missing record property");
    }
    String localId = inventoryPayload.getString("localIdentifier");
    if (localId == null) {
      throw new IllegalArgumentException("inventory xml: missing record/localIdentifier string");
    }
    inventoryPayload.remove("original");
    return new JsonObject()
        .put("localId", localId)
        .put("payload", new JsonObject()
            .put("marc", marcPayload)
            .put("inventory", inventoryPayload));
  }

  /**
   * Create ingest object with "localId", "marcPayload", "inventoryPayload".
   * @param marcXml MARC XML string
   * @param templates List of XSLT templates to apply
   * @return ingest JSON object
   * @throws TransformerException transformer problem
   * @throws XMLStreamException xml stream problem (Invalid XML)
   */
  public static JsonObject createIngestRecord(String marcXml, List<Templates> templates)
      throws TransformerException, XMLStreamException {

    String inventory = marcXml;
    for (Templates template : templates) {
      Source source = new StreamSource(new StringReader(inventory));
      StreamResult result = new StreamResult(new StringWriter());
      Transformer transformer = template.newTransformer();
      transformer.transform(source, result);
      inventory = result.getWriter().toString();
    }
    return createIngestRecord(MarcXmlToJson.convert(marcXml),
        XmlJsonUtil.inventoryXmlToJson(inventory));
  }

  /**
   * Create MARC tag with indicators given.
   * @param marc MARC-in-JSON object
   * @param tag marc tag, such as "245"
   * @param ind1 indicator 1
   * @param ind2 indicator 2
   * @return fields array
   */
  public static JsonArray createMarcDataField(JsonObject marc, String tag,
      String ind1, String ind2) {

    JsonArray fields = marc.getJsonArray(FIELDS_LABEL);
    if (fields == null) {
      fields = new JsonArray();
      marc.put(FIELDS_LABEL, fields);
    }
    int i;
    for (i = 0; i < fields.size(); i++) {
      JsonObject field = fields.getJsonObject(i);
      int cmp = 1;
      for (String f : field.fieldNames()) {
        cmp = tag.compareTo(f);
        if (cmp <= 0) {
          break;
        }
      }
      if (cmp <= 0) {
        break;
      }
    }
    JsonObject field = new JsonObject();
    fields.add(i, new JsonObject().put(tag, field));
    field.put("ind1", ind1);
    field.put("ind2", ind2);
    JsonArray subfields = new JsonArray();
    field.put(SUBFIELDS_LABEL, subfields);
    return subfields;
  }

  /**
   * Lookup marc field.
   * @param marc MARC-in-JSON object
   * @param tag marc tag, such as "245"
   * @param ind1 indicator1 in match ; null for any
   * @param ind2 indicator2 in match; null for any
   * @return subfields array if found; null otherwise
   */
  public static JsonArray lookupMarcDataField(JsonObject marc, String tag,
      String ind1, String ind2) {
    JsonArray fields = marc.getJsonArray(FIELDS_LABEL);
    if (fields == null) {
      return null;
    }
    for (int i = 0; i < fields.size(); i++) {
      JsonObject field = fields.getJsonObject(i);
      for (String f : field.fieldNames()) {
        if (f.equals(tag)) {
          JsonObject field2 = field.getJsonObject(tag);
          if ((ind1 == null || ind1.equals(field2.getString("ind1")))
              && (ind2 == null || ind2.equals(field2.getString("ind2")))) {
            return field2.getJsonArray(SUBFIELDS_LABEL);
          }
        }
      }
    }
    return null;
  }

  /**
   * Remove tag from record.
   * @param marc MARC-in-JSON object
   * @param tag fields with this tag are removed
   */
  public static void removeMarcField(JsonObject marc, String tag) {
    JsonArray fields = marc.getJsonArray(FIELDS_LABEL);
    if (fields == null) {
      return;
    }
    int i = 0;
    while (i < fields.size()) {
      JsonObject field = fields.getJsonObject(i);
      int cmp = 1;
      for (String f : field.fieldNames()) {
        cmp = tag.compareTo(f);
        if (cmp == 0) {
          break;
        }
      }
      if (cmp == 0) {
        fields.remove(i);
      } else {
        i++;
      }
    }
  }

}
