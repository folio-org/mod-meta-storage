package org.folio.metastorage.util;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;

import io.vertx.core.json.JsonObject;
import org.junit.Test;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class OaiParserTest {

  OaiParser<String> createStringParser() {
    OaiParser<String> oaiParser = new OaiParser();
    oaiParser.setParseMetadata(x -> {
      try {
        return XmlJsonUtil.getSubDocument(x.next(), x);
      } catch (XMLStreamException e) {
        throw new RuntimeException(e);
      }
    });
    return oaiParser;
  }

  @Test
  public void listRecords1() throws FileNotFoundException, XMLStreamException {
    OaiParser<String> oaiParser = createStringParser();
    InputStream stream = new FileInputStream("src/test/resources/oai-response-1.xml");
    assertThat(oaiParser.getResumptionToken(), nullValue());
    List<OaiRecord<String>> records = new LinkedList<>();
    oaiParser.parseResponse(stream, records::add);
    assertThat(records, hasSize(4));
    assertThat(records.get(0).isDeleted, is(true));
    assertThat(records.get(1).isDeleted, is(false));
    assertThat(records.get(2).isDeleted, is(false));
    assertThat(records.get(3).isDeleted, is(false));
    assertThat(records.get(0).identifier, is("998212783503681"));
    assertThat(records.get(1).identifier, is("9977919382003681"));
    assertThat(records.get(2).identifier, is("9977924842403681"));
    assertThat(records.get(3).identifier, is("9977648149503681"));
    assertThat(records.get(0).metadata, nullValue());
    assertThat(records.get(1).metadata, containsString("<record xmlns=\"http://www.loc.gov/MARC21/slim\"><leader>10873cam a22004693i 4500</leader>"));
    assertThat(records.get(2).metadata, containsString("02052cam"));
    assertThat(records.get(3).metadata, containsString("02225nam"));
    assertThat(oaiParser.getResumptionToken(), is("MzM5OzE7Ozt2MS4w"));
    assertThat(records.get(3).datestamp, is("2022-05-03"));
    oaiParser.clear();
    assertThat(oaiParser.getResumptionToken(), nullValue());
  }

  @Test
  public void listRecords2() throws FileNotFoundException, XMLStreamException {
    OaiParser<String> oaiParser = createStringParser();
    InputStream stream = new FileInputStream("src/test/resources/oai-response-2.xml");
    List<OaiRecord<String>> records = new LinkedList<>();
    oaiParser.parseResponse(stream, records::add);
    assertThat(records, empty());
    assertThat(oaiParser.getResumptionToken(), nullValue());
  }

  @Test
  public void listRecords3() throws FileNotFoundException, XMLStreamException {
    OaiParser<String> oaiParser = createStringParser();
    InputStream stream = new FileInputStream("src/test/resources/oai-response-3.xml");
    List<OaiRecord<String>> records = new LinkedList<>();
    oaiParser.parseResponse(stream, records::add);
    assertThat(oaiParser.getResumptionToken(), is("MzM5OzE7Ozt2MS4w"));
    assertThat(records, hasSize(1));
    assertThat(records.get(0).getMetadata(), nullValue());
    assertThat(records.get(0).getIsDeleted(), is(true));
    assertThat(records.get(0).getDatestamp(), is("2022-05-03"));
    assertThat(records.get(0).getIdentifier(), is("998212783503681"));
  }

  @Test
  public void listRecords4() throws FileNotFoundException, XMLStreamException {
    OaiParser<String> oaiParser = createStringParser();
    InputStream stream = new FileInputStream("src/test/resources/oai-response-4.xml");
    assertThat(oaiParser.getResumptionToken(), nullValue());
    List<OaiRecord<String>> records = new LinkedList<>();
    oaiParser.parseResponse(stream, records::add);
    assertThat(records, hasSize(4));
    assertThat(records.get(0).isDeleted, is(true));
    assertThat(records.get(1).isDeleted, is(false));
    assertThat(records.get(2).isDeleted, is(false));
    assertThat(records.get(3).isDeleted, is(false));
    assertThat(records.get(0).identifier, is("998212783503681"));
    assertThat(records.get(1).identifier, is("9977919382003681"));
    assertThat(records.get(2).identifier, is("9977924842403681"));
    assertThat(records.get(3).identifier, is("9977648149503681"));
    assertThat(records.get(0).metadata, nullValue());
    assertThat(records.get(1).metadata, nullValue());
    assertThat(records.get(2).metadata, nullValue());
    assertThat(records.get(3).metadata, nullValue());
    assertThat(oaiParser.getResumptionToken(), is("MzM5OzE7Ozt2MS4w"));
    assertThat(records.get(3).datestamp, is("2022-05-03"));
  }

  @Test
  public void listRecordsJson() throws FileNotFoundException, XMLStreamException {
    OaiParser<JsonObject> oaiParser = new OaiParser<>();
    InputStream stream = new FileInputStream("src/test/resources/oai-response-1.xml");
    XMLInputFactory factory = XMLInputFactory.newInstance();
    factory.setProperty(XMLInputFactory.SUPPORT_DTD, false);
    XMLStreamReader xmlStreamReader = factory.createXMLStreamReader(stream);

    List<OaiRecord<JsonObject>> records = new LinkedList<>();
    oaiParser.setParseMetadata(x -> XmlJsonUtil.convertMarcXmlToJson(x));
    oaiParser.parseResponse(xmlStreamReader, records::add);
    assertThat(records, hasSize(4));
    assertThat(records.get(0).isDeleted, is(true));
    assertThat(records.get(1).isDeleted, is(false));
    assertThat(records.get(2).isDeleted, is(false));
    assertThat(records.get(3).isDeleted, is(false));
    assertThat(records.get(0).identifier, is("998212783503681"));
    assertThat(records.get(1).identifier, is("9977919382003681"));
    assertThat(records.get(2).identifier, is("9977924842403681"));
    assertThat(records.get(3).identifier, is("9977648149503681"));
    assertThat(records.get(0).metadata, nullValue());
    assertThat(records.get(1).metadata.encode(), containsString("{\"leader\":\"10873cam a22004693i 4500\",\"fields\":[{\"001\":\"9977919382003681\"}"));
    assertThat(records.get(2).metadata.encode(), containsString("02052cam"));
    assertThat(records.get(3).metadata.encode(), containsString("02225nam"));
    assertThat(oaiParser.getResumptionToken(), is("MzM5OzE7Ozt2MS4w"));
    assertThat(records.get(3).datestamp, is("2022-05-03"));
  }
}
