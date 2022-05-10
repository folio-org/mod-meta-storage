package org.folio.metastorage.matchkey;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import java.util.Collection;
import java.util.HashSet;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.testcontainers.shaded.com.google.common.io.Resources;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

@RunWith(VertxUnitRunner.class)
public class MatchKeyJavaScriptTest {

  static Vertx vertx;

  @BeforeClass
  public static void beforeClass()  {
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
  public void testNoSuchFile(TestContext context) {
    MatchKeyMethod.get("javascript", new JsonObject()
            .put("filename", "isbn-match-no.js"))
        .onComplete(context.asyncAssertFailure(e ->
            assertThat(e.getMessage(), containsString("isbn-match-no.js"))
        ));
  }

  @Test
  public void testLong(TestContext context) {
    Collection<String> keys = new HashSet<>();
    MatchKeyMethod.get("javascript", new JsonObject()
            .put("script", "x => JSON.parse(x).id + 1"))
        .onComplete(context.asyncAssertSuccess(matchKeyMethod -> {
          matchKeyMethod.getKeys(new JsonObject().put("id", 2), keys);
          assertThat(keys, containsInAnyOrder("3"));
        }));
  }

  @Test
  public void testString(TestContext context) {
    Collection<String> keys = new HashSet<>();
    MatchKeyMethod.get("javascript", new JsonObject()
            .put("script", "x => JSON.parse(x).id + 'x'"))
        .onComplete(context.asyncAssertSuccess(matchKeyMethod -> {
          matchKeyMethod.getKeys(new JsonObject().put("id", "2"), keys);
          assertThat(keys, containsInAnyOrder("2x"));
        }));
  }

  @Test
  public void testBoolean(TestContext context) {
    Collection<String> keys = new HashSet<>();
    MatchKeyMethod.get("javascript", new JsonObject()
            .put("script", "x => JSON.parse(x).id > 1"))
        .onComplete(context.asyncAssertSuccess(matchKeyMethod -> {
          matchKeyMethod.getKeys(new JsonObject().put("id", "2"), keys);
          assertThat(keys, containsInAnyOrder());
        }));
  }

  @Test
  public void testArray(TestContext context) {
    Collection<String> keys = new HashSet<>();
    MatchKeyMethod.get("javascript", new JsonObject()
            .put("script", "function mult(p1, p2) { return p1 * p2; };"
            + " x => [JSON.parse(x).id, mult(2, 3)]"))
        .onComplete(context.asyncAssertSuccess(matchKeyMethod -> {
          matchKeyMethod.getKeys(new JsonObject().put("id", "2"), keys);
          assertThat(keys, containsInAnyOrder("6", "2"));
        }));
  }

  @Test
  public void testInventoryInstnanceTitle(TestContext context) {

    JsonObject inventory = new JsonObject()
        .put("identifiers", new JsonArray()
            .add(new JsonObject()
                .put("isbn", "73209629"))
            .add(new JsonObject()
                .put("isbn", "73209623"))

        );

    Collection<String> keys = new HashSet<>();
    MatchKeyMethod.get("javascript", new JsonObject()
            .put("filename", Resources.getResource("isbn-match.js").getFile()))
        .onComplete(context.asyncAssertSuccess(matchKeyMethod -> {
          matchKeyMethod.getKeys(inventory, keys);
          assertThat(keys, containsInAnyOrder("73209629", "73209623"));
        }));
  }

}
