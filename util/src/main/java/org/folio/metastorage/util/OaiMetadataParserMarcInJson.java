package org.folio.metastorage.util;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;

public class OaiMetadataParserMarcInJson implements OaiMetadataParser<JsonObject> {

  JsonObject marc = new JsonObject();

  JsonArray fields = new JsonArray();

  JsonArray subFields;

  int recordNo;

  String tag;

  String elem;

  String code;

  StringBuilder cdata = new StringBuilder();

  @Override
  public void init() {
    cdata.setLength(0);
    elem = null;
    marc = new JsonObject();
    fields = new JsonArray();
    recordNo = 0;
  }

  private static final String COLLECTION_LABEL = "collection";
  private static final String RECORD_LABEL = "record";
  private static final String LEADER_LABEL = "leader";
  private static final String DATAFIELD_LABEL = "datafield";
  private static final String CONTROLFIELD_LABEL = "controlfield";
  private static final String TAG_LABEL = "tag";
  private static final String SUBFIELD_LABEL = "subfield";
  private static final String SUBFIELDS_LABEL = "subfields";
  private static final String CODE_LABEL = "code";
  private static final String FIELDS_LABEL = "fields";

  static String getAttribute(XMLStreamReader xmlStreamReader, String name) {
    for (int i = 0; i < xmlStreamReader.getAttributeCount(); i++) {
      if (name.equals(xmlStreamReader.getAttributeLocalName(i))) {
        return xmlStreamReader.getAttributeValue(i);
      }
    }
    return null;
  }

  void endElement() {
    if (elem != null) {
      String val = cdata.toString();
      switch (elem) {
        case LEADER_LABEL -> marc.put(LEADER_LABEL, val);
        case CONTROLFIELD_LABEL -> fields.add(new JsonObject().put(tag, val));
        case SUBFIELD_LABEL -> subFields.add(new JsonObject().put(code, val));
      }
      elem = null;
    }
    cdata.setLength(0);
  }

  @Override
  public void handle(XMLStreamReader stream) {
    int event = stream.getEventType();
    if (event == XMLStreamConstants.START_ELEMENT) {
      endElement();
      elem = stream.getLocalName();
      if (RECORD_LABEL.equals(elem)) {
        recordNo++;
        if (recordNo > 1) {
          throw new IllegalArgumentException("can not handle multiple records");
        }
      } else if (LEADER_LABEL.equals(elem)) {
        ;
      } else if (CONTROLFIELD_LABEL.equals(elem)) {
        tag = getAttribute(stream, TAG_LABEL);
      } else if (DATAFIELD_LABEL.equals(elem)) {
        JsonObject field = new JsonObject();
        for (int j = 1; j <= 9; j++) { // ISO 2709 allows more than 2 indicators
          String ind = getAttribute(stream, "ind" + j);
          if (ind != null) {
            field.put("ind" + j, ind);
          }
        }
        subFields = new JsonArray();
        field.put(SUBFIELDS_LABEL, subFields);
        tag = getAttribute(stream, TAG_LABEL);
        fields.add(new JsonObject().put(tag, field));
      } else if (SUBFIELD_LABEL.equals(elem)) {
        code = getAttribute(stream, CODE_LABEL);
        if (subFields == null) {
          throw new IllegalArgumentException("subfield without field");
        }
      } else if (!COLLECTION_LABEL.equals(elem)) {
        throw new IllegalArgumentException("Bad marcxml element: " + elem);
      }
    } else if (event == XMLStreamConstants.END_ELEMENT) {
      endElement();
    } else if (event == XMLStreamConstants.CHARACTERS) {
      cdata.append(stream.getText());
    }
  }

  @Override
  public JsonObject result() {
    if (recordNo == 0) {
      throw new IllegalArgumentException("No record element found");
    }
    if (!fields.isEmpty()) {
      marc.put(FIELDS_LABEL, fields);
    }
    return marc;
  }
}
