package org.folio.metastorage.util;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.file.OpenOptions;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import java.util.ArrayList;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import javax.xml.stream.XMLStreamConstants;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;

@RunWith(VertxUnitRunner.class)
public class XmlParserTest {
  Vertx vertx;
  @Before
  public void before() {
    vertx = Vertx.vertx();
  }

  @After
  public void after(TestContext context) {
    vertx.close().onComplete(context.asyncAssertSuccess());
  }

  Future<List<Integer>> eventsFromFile(String fname) {
    return vertx.fileSystem().open(fname, new OpenOptions())
        .compose(asyncFile -> {
          List<Integer> events = new ArrayList<>();
          Promise<List<Integer>> promise = Promise.promise();
          XmlParser xmlParser = XmlParser.newParser(asyncFile);
          xmlParser.handler(event -> events.add(event.getEventType()));
          xmlParser.endHandler(e -> promise.complete(events));
          xmlParser.exceptionHandler(e -> promise.tryFail(e));
          return promise.future();
        });
  }

  @Test
  public void record10(TestContext context) {
    eventsFromFile("record10.xml").onComplete(context.asyncAssertSuccess(events -> {
      assertThat(events, hasSize(2965));
    }));
  }

  @Test
  public void small(TestContext context) {
    eventsFromFile("small.xml").onComplete(context.asyncAssertSuccess(events -> {
      assertThat(events, contains(XMLStreamConstants.START_DOCUMENT,
          XMLStreamConstants.START_ELEMENT, XMLStreamConstants.END_ELEMENT,
          XMLStreamConstants.END_DOCUMENT));
    }));
  }

  @Test
  public void bad(TestContext context) {
    eventsFromFile("bad.xml").onComplete(context.asyncAssertFailure(e -> {
      assertThat(e.getMessage(), containsString("Unexpected character '<'"));
    }));
  }

}
