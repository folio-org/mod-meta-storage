package org.folio.metastorage.module.impl;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.ext.web.Router;
import io.vertx.reactivex.core.http.HttpHeaders;
import java.util.UUID;
import org.folio.metastorage.module.ModuleCache;
import org.folio.metastorage.server.entity.ClusterBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;


@RunWith(VertxUnitRunner.class)
public class ModuleTest {

  static Vertx vertx;

  static HttpServer httpServer;

  static int PORT = 9231;

  static String HOSTPORT = "http://localhost:" + PORT;

  static String TENANT = "test";

  @BeforeClass
  public static void beforeClass(TestContext context)  {
    vertx = Vertx.vertx();
    Router router = Router.router(vertx);
    router.get("/lib/isbn-transformer.mjs").handler(ctx -> {
      HttpServerResponse response = ctx.response();
      response.setStatusCode(200);
      response.putHeader(HttpHeaders.CONTENT_TYPE, "text/plain");
      response.end("""
        export function transform(clusterStr) {
          let cluster = JSON.parse(clusterStr);
          let recs = cluster.records;
          //merge all marc recs
          const out = {};
          out.leader = 'new leader';
          out.fields = [];
          for (let i = 0; i < recs.length; i++) {
            let rec = recs[i];
            let marc = rec.payload.marc;
            //collect all marc fields
            out.fields.push(...marc.fields);
            //stamp with custom 999 for each member
            let f999 = 
            {
              '999' : 
              {
                'ind1': '1',
                'ind2': '0',
                'subfields': [
                  {'i': rec.globalId },
                  {'l': rec.localId },
                  {'s': rec.sourceId }
                ]
              }
            };
            out.fields.push(f999);
          }
          return JSON.stringify(out);
        }
        """);
    });
    httpServer = vertx.createHttpServer();
    httpServer.requestHandler(router).listen(PORT).onComplete(context.asyncAssertSuccess());
  }

  @AfterClass
  public static void afterClass(TestContext context) {
    vertx.close().onComplete(context.asyncAssertSuccess());
  }


  @Test
  public void testIsbnTransformerUrl(TestContext context) {
    JsonObject inputOld = new JsonObject()
        .put("identifiers", new JsonArray()
            .add(new JsonObject()
                .put("isbn", "73209629"))
            .add(new JsonObject()
                .put("isbn", "73209623"))

        );

    JsonArray recordsIn = new JsonArray()
      //first record
      .add(new JsonObject()
        .put("globalId", "source-1-record-1")
        .put("localId", "REC:A")
        .put("sourceId", "source-1")
        .put("payload", new JsonObject()
          .put("marc", new JsonObject()
            .put("leader", "leader-1")
            .put("fields", new JsonArray()
              .add(new JsonObject()
                .put("245", new JsonObject()
                  .put("subfields", new JsonArray()
                    .add(new JsonObject().put("a", "source-1 title"))
                  )
                )
              )
              .add(new JsonObject()
                .put("998", new JsonObject()
                  .put("subfields", new JsonArray()
                    .add(new JsonObject().put("x", "source-1 location"))
                  )
                )
              )
            )
          )
        )
      )
      //second record
      .add(new JsonObject()
        .put("globalId", "source-2-record-2")
        .put("localId", "rec_1")
        .put("sourceId", "source-2")
        .put("payload", new JsonObject()
          .put("marc", new JsonObject()
            .put("leader", "leader-1")
            .put("fields", new JsonArray()
              .add(new JsonObject()
                .put("245", new JsonObject()
                  .put("subfields", new JsonArray()
                    .add(new JsonObject().put("a", "source-2 title"))
                  )
                )
              )
              .add(new JsonObject()
                .put("998", new JsonObject()
                  .put("subfields", new JsonArray()
                    .add(new JsonObject().put("x", "source-2 location"))
                  )
                )
              )
            )
          )
        )
      );

      JsonObject recordOut = new JsonObject()
      //merged record
        .put("leader", "new leader")
        .put("fields", new JsonArray()
          .add(new JsonObject()
            .put("245", new JsonObject()
              .put("subfields", new JsonArray()
                .add(new JsonObject().put("a", "source-1 title"))
              )
            )
          )
          .add(new JsonObject()
            .put("998", new JsonObject()
              .put("subfields", new JsonArray()
                .add(new JsonObject().put("x", "source-1 location"))
              )
            )
          )
          .add(new JsonObject()
            .put("999", new JsonObject()
              .put("ind1", "1")
              .put("ind2", "0")
              .put("subfields", new JsonArray()
                .add(new JsonObject().put("i", "source-1-record-1"))
                .add(new JsonObject().put("l", "REC:A"))
                .add(new JsonObject().put("s", "source-1"))
              )
            )
          )
          .add(new JsonObject()
            .put("245", new JsonObject()
              .put("subfields", new JsonArray()
                .add(new JsonObject().put("a", "source-2 title"))
              )
            )
          )
          .add(new JsonObject()
            .put("998", new JsonObject()
              .put("subfields", new JsonArray()
                .add(new JsonObject().put("x", "source-2 location"))
              )
            )
          )
          .add(new JsonObject()
            .put("999", new JsonObject()
              .put("ind1", "1")
              .put("ind2", "0")
              .put("subfields", new JsonArray()
                .add(new JsonObject().put("i", "source-2-record-2"))
                .add(new JsonObject().put("l", "rec_1"))
                .add(new JsonObject().put("s", "source-2"))
              )
            )
          )
        );
  

    ClusterBuilder cb = new ClusterBuilder(UUID.randomUUID());
    cb.records(recordsIn);
    JsonObject input = cb.build();

    JsonObject config = new JsonObject()
      .put("id", "isbn-transformer")
      .put("url", HOSTPORT + "/lib/isbn-transformer.mjs")
      .put("function", "transform");
      
    ModuleCache.getInstance().lookup(vertx, TENANT, config)
      //.onSuccess(m -> m.terminate())
      .compose(m -> m.execute(input))
      .onComplete(context.asyncAssertSuccess(output -> { 
          context.assertEquals(recordOut, output);
        }
      )
    );
  }

}
