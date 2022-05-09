package org.folio.metastorage.matchkey;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import java.util.Collection;
import java.util.HashSet;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;

@RunWith(VertxUnitRunner.class)
public class MatchKeyJavaScriptTest {

  static Vertx vertx;

  @BeforeClass
  public static void beforeClass(TestContext context)  {
    vertx = Vertx.vertx();
  }

  @Test
  public void testMissingConfig(TestContext context) {
    MatchKeyMethod.get("javascript", new JsonObject())
        .onComplete(context.asyncAssertFailure(e ->
          assertThat(e.getMessage(), is("javascript: filename or script must be given"))
        ));
  }

  @Test
  public void testBadJavaScript(TestContext context) {
    MatchKeyMethod.get("javascript", new JsonObject().put("script", "x =>"))
        .onComplete(context.asyncAssertFailure(e ->
            assertThat(e.getMessage(), containsString("Expected an operand but found eof"))
        ));
  }

  @Test
  public void testLong(TestContext context) {
    Collection<String> keys = new HashSet<>();
    MatchKeyMethod.get("javascript", new JsonObject().put("script", "x => x + 1"))
        .onComplete(context.asyncAssertSuccess(matchKeyMethod -> {
          matchKeyMethod.getKeys(new JsonObject(), keys);
          assertThat(keys, contains("2"));
        }));
  }

}
