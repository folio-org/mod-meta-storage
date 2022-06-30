package org.folio.metastorage.server;

import org.folio.metastorage.server.entity.ClusterBuilder;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

public class ClusterRecord {

  UUID clusterId;
  List<String> clusterValues;
  LocalDateTime datestamp;
  String oaiSet;
  ClusterBuilder cb;
  String metadata;
}
