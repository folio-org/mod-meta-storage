package org.folio.metastorage.server;

import io.vertx.codegen.annotations.Nullable;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.WriteStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.metastorage.module.Module;
import org.folio.metastorage.util.JsonToMarcXml;

import java.util.HashSet;
import java.util.Set;

import static org.folio.metastorage.server.OaiService.encodeOaiIdentifier;
import static org.folio.metastorage.server.OaiService.getMetadata;
import static org.folio.metastorage.util.EncodeXmlText.encodeXmlText;

public class ClusterRecordStream implements WriteStream<ClusterRecord> {

  private static final Logger log = LogManager.getLogger(ClusterRecordStream.class);
  boolean ended;

  Set<ClusterRecord> work = new HashSet<>();

  boolean withMetadata;

  Handler<Void> drainHandler;

  Handler<Throwable> exceptionHandler;

  Module module;

  WriteStream<Buffer> response;

  int writeQueueMaxSize = 5;

  public ClusterRecordStream(WriteStream<Buffer> response, Module module, boolean withMetadata) {
    this.response = response;
    this.module = module;
    this.withMetadata = withMetadata;
  }

  @Override
  public WriteStream<ClusterRecord> exceptionHandler(Handler<Throwable> handler) {
    this.exceptionHandler = handler;
    return this;
  }

  Future<Void> perform(ClusterRecord cr) {
    if (cr.cb == null) {
      return Future.succeededFuture(null);
    }
    Future<Void> future;
    try {
      if (module == null) {
        cr.metadata = getMetadata(cr.cb.build());
        future = Future.succeededFuture();
      } else {
        future = module.execute(cr.cb.build())
            .map(json -> {
              cr.metadata = JsonToMarcXml.convert(json);
              return null;
            });
      }
    } catch (Exception e) {
      future = Future.failedFuture(e);
    }
    return future.compose(x -> {
      String begin = withMetadata ? "    <record>\n" : "";
      String end = withMetadata ? "    </record>\n" : "";
      Buffer buffer = Buffer.buffer(
          begin
              + "      <header" + (cr.metadata == null
              ? " status=\"deleted\"" : "") + ">\n"
              + "        <identifier>"
              + encodeXmlText(encodeOaiIdentifier(cr.clusterId)) + "</identifier>\n"
              + "        <datestamp>"
              + encodeXmlText(Util.formatOaiDateTime(cr.datestamp))
              + "</datestamp>\n"
              + "        <setSpec>" + encodeXmlText(cr.oaiSet) + "</setSpec>\n"
              + "      </header>\n"
              + (withMetadata && cr.metadata != null
              ? "    <metadata>\n" + cr.metadata + "\n"
              + "    </metadata>\n"
              : "")
              + end);
      response.write(buffer);
      return Future.succeededFuture();
    }, e -> {
      log.info("failure {}", e.getMessage(), e);
      response.write(Buffer.buffer("<!-- Failed to produce record: "
          + encodeXmlText(e.getMessage()) + " -->\n"));
      return Future.succeededFuture();
    });
  }

  @Override
  public Future<Void> write(ClusterRecord cr) {
    work.add(cr);
    log.info("write {} begin", cr);
    perform(cr).onComplete(x -> {
      log.info("write {} done", cr);
      work.remove(cr);
      if (work.size() == writeQueueMaxSize - 1) {
        drainHandler.handle(null);
      }
      if (work.isEmpty() && ended) {
        log.info("end in write");
        this.endHandler.handle(Future.succeededFuture());
      }
    });
    return Future.succeededFuture();
  }

  @Override
  public void write(ClusterRecord clusterRecord, Handler<AsyncResult<Void>> handler) {
    write(clusterRecord).onComplete(handler);
  }

  Handler<AsyncResult<Void>> endHandler;

  @Override
  public void end(Handler<AsyncResult<Void>> handler) {
    log.info("end 1");
    if (ended) {
      throw new IllegalStateException("already ended");
    }
    ended = true;
    this.endHandler = handler;
    if (work.isEmpty()) {
      log.info("end immediately");
      this.endHandler.handle(Future.succeededFuture());
    }
  }

  @Override
  public WriteStream<ClusterRecord> setWriteQueueMaxSize(int i) {
    writeQueueMaxSize = i;
    return this;
  }

  @Override
  public boolean writeQueueFull() {
    return work.size() >= writeQueueMaxSize;
  }

  @Override
  public WriteStream<ClusterRecord> drainHandler(@Nullable Handler<Void> handler) {
    this.drainHandler = handler;
    return this;
  }

}
