package org.folio.metastorage.matchkey;

import com.jayway.jsonpath.InvalidPathException;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.util.HashSet;
import java.util.Set;

import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.metastorage.matchkey.impl.MatchKeyJsonPath;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

@RunWith(VertxUnitRunner.class)
public class MatchKeyJsonPathTest {

  static Vertx vertx;

  @BeforeClass
  public static void beforeClass(TestContext context)  {
    vertx = Vertx.vertx();
  }

  @Test
  public void matchKeyJsonPathNonConfigured() {
    MatchKeyMethod matchKeyMethod = new MatchKeyJsonPath();
    JsonObject payload = new JsonObject();
    Set<String> keys = new HashSet<>();
    Exception e = Assert.assertThrows(
        IllegalArgumentException.class,
        () -> matchKeyMethod.getKeys(payload, keys));
    assertThat(e.getMessage(), is("path can not be null"));
  }

  @Test
  public void matchKeyJsonPathNonConfigured(TestContext context) {
    MatchKeyMethod.get(vertx, "foo", new JsonObject()).onComplete(context.asyncAssertFailure(e ->
        assertThat(e.getMessage(), is("Unknown match key method foo"))
    ));
  }

  @Test
  public void matchKeyBadPath(TestContext context) {
    MatchKeyMethod.get(vertx, "jsonpath", new JsonObject()).onComplete(context.asyncAssertFailure(e ->
      assertThat(e.getMessage(), is("jsonpath: expr must be given"))
    ));
  }

  @Test
  public void matchKeyJsonPathConfigureInvalidJsonPath() {
    MatchKeyMethod matchKeyMethod = new MatchKeyJsonPath();
    JsonObject configuration = new JsonObject().put("expr", "$.fields.010.subfields[x");
    Assert.assertThrows(InvalidPathException.class,
        () -> matchKeyMethod.configure(configuration));
  }

  @Test
  public void matchKeyJsonPathConfigureInvalidJsonPath2(TestContext context) {
    JsonObject configuration = new JsonObject().put("expr", "$.fields.010.subfields[x");
    MatchKeyMethod.get(vertx, "jsonpath", configuration).onComplete(context.asyncAssertFailure(e ->
            assertThat(e.getClass(), is(InvalidPathException.class))
        ));
  }

  @Test
  public void matchKeyJsonPathConfigureMarc(TestContext context) {
    MatchKeyMethod matchKeyMethod = new MatchKeyJsonPath();
    matchKeyMethod.configure(new JsonObject().put("expr", "$.marc.fields.010.subfields[*].a"))
        .onComplete(context.asyncAssertSuccess(s -> {

          JsonObject payload = new JsonObject()
              .put("marc", new JsonObject()
                  .put("leader", "00942nam  22002531a 4504")
                  .put("fields", new JsonObject()
                      .put("001", "   73209622 //r823")
                      .put("010", new JsonObject()
                          .put("subfields", new JsonArray()
                              .add(new JsonObject().put("b", "73209622"))
                          )
                      )
                      .put("245", new JsonObject()
                          .put("subfields", new JsonArray()
                              .add(new JsonObject().put("a", "The Computer Bible /"))
                              .add(new JsonObject().put("c", "J. Arthur Baird, David Noel Freedman, editors." ))
                          )
                      )
                  )
              );
          Set<String> keys = new HashSet<>();
          matchKeyMethod.getKeys(payload, keys);
          assertThat(keys, is(empty()));

          payload = new JsonObject()
              .put("marc", new JsonObject()
                  .put("leader", "00942nam  22002531a 4504")
                  .put("fields", new JsonObject()
                      .put("001", "   73209622 //r823")
                      .put("010", new JsonObject()
                          .put("subfields", new JsonArray()
                              .add(new JsonObject().put("a", "73209622"))
                              .add(new JsonObject().put("a", "73209623"))
                          )
                      )
                      .put("245", new JsonObject()
                          .put("subfields", new JsonArray()
                              .add(new JsonObject().put("a", "The Computer Bible /"))
                              .add(new JsonObject().put("c", "J. Arthur Baird, David Noel Freedman, editors." ))
                          )
                      )
                  ));
          keys.clear();
          matchKeyMethod.getKeys(payload, keys);
          assertThat(keys, containsInAnyOrder("73209622", "73209623"));
        }));
  }

  @Test
  public void matchKeyJsonPathConfigureInventory(TestContext context) {
    MatchKeyMethod matchKeyMethod = new MatchKeyJsonPath();
    matchKeyMethod.configure(new JsonObject().put("expr", "$.inventory.isbn[*]"))
        .onComplete(context.asyncAssertSuccess(s -> {
          JsonObject payload = new JsonObject()
              .put("inventory", new JsonObject()
                  .put("isbn", new JsonArray().add("73209622")));
          Set<String> keys = new HashSet<>();
          matchKeyMethod.getKeys(payload, keys);
          assertThat(keys, contains("73209622"));

          payload = new JsonObject()
              .put("inventory", new JsonObject()
                  .put("issn", new JsonArray().add("73209622")));
          keys.clear();
          matchKeyMethod.getKeys(payload, keys);
          assertThat(keys, is(empty()));
        }));
  }

  Future<Void> matchKeyVerify(String pattern, Set<String> expectedKeys, JsonObject payload) {
    return MatchKeyMethod.get(vertx, "jsonpath", new JsonObject().put("expr", pattern))
        .map(matchKeyMethod -> {
          Set<String> keys = new HashSet<>();
          matchKeyMethod.getKeys(payload, keys);
          Assert.assertEquals(expectedKeys, keys);
          return null;
        });
  }

  @Test
  public void matchKeyJsonPathExpressions(TestContext context) {
    JsonObject inventory = new JsonObject()
        .put("identifiers", new JsonArray()
            .add(new JsonObject()
                .put("isbn", "73209629"))
            .add(new JsonObject()
                .put("isbn", "73209623"))

        )
        .put("matchKey", new JsonObject()
            .put("title", "Panisci fistula")
            .put("remainder-of-title", " : tre preludi per tre flauti")
            .put("medium", "[sound recording]")
        )
        ;

    matchKeyVerify("$.identifiers[*].isbn", Set.of("73209629", "73209623"), inventory)
        .compose(x -> matchKeyVerify("$.matchKey.title", Set.of("Panisci fistula"), inventory))
        .compose(x -> matchKeyVerify("$.matchKey", Set.of(), inventory))
        .compose(x -> matchKeyVerify("$.matchKey[?(@.title)]", Set.of(), inventory))
        .onComplete(context.asyncAssertSuccess());
  }

}
