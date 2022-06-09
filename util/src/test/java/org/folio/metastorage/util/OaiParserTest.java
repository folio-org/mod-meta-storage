package org.folio.metastorage.util;

import org.junit.Test;

import javax.xml.stream.XMLStreamException;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class OaiParserTest {

  @Test
  public void listRecords1() throws FileNotFoundException, XMLStreamException {
    OaiParser oaiParser = new OaiParser();
    InputStream stream = new FileInputStream("src/test/resources/oai-response-1.xml");
    assertThat(oaiParser.getIdentifiers(), empty());
    assertThat(oaiParser.getMetadata(), empty());
    assertThat(oaiParser.getResumptionToken(), nullValue());
    oaiParser.applyResponse(stream);
    assertThat(oaiParser.getIdentifiers(), hasSize(4));
    assertThat(oaiParser.getIdentifiers(), contains("998212783503681", "9977919382003681", "9977924842403681", "9977648149503681"));
    assertThat(oaiParser.getMetadata(), hasSize(4));
    assertThat(oaiParser.getMetadata().get(0), nullValue());
    assertThat(oaiParser.getMetadata().get(1), containsString("<record xmlns=\"http://www.loc.gov/MARC21/slim\"><leader>10873cam a22004693i 4500</leader>"));
    assertThat(oaiParser.getMetadata().get(2), containsString("02052cam"));
    assertThat(oaiParser.getMetadata().get(3), containsString("02225nam"));
    assertThat(oaiParser.getResumptionToken(), is("MzM5OzE7Ozt2MS4w"));
    oaiParser.clear();
    assertThat(oaiParser.getIdentifiers(), empty());
    assertThat(oaiParser.getMetadata(), empty());
    assertThat(oaiParser.getResumptionToken(), nullValue());
  }

  @Test
  public void listRecords2() throws FileNotFoundException, XMLStreamException {
    OaiParser oaiParser = new OaiParser();
    InputStream stream = new FileInputStream("src/test/resources/oai-response-2.xml");
    oaiParser.applyResponse(stream);
    assertThat(oaiParser.getIdentifiers(), empty());
    assertThat(oaiParser.getMetadata(), empty());
    assertThat(oaiParser.getResumptionToken(), nullValue());
  }

}
