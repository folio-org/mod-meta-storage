package org.folio.metastorage.server.entity;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.LocalDateTime;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class OaiPmhStatus {

  String error;

  LocalDateTime lastActiveTimestamp;

  Integer lastRecsPerSec;

  LocalDateTime lastStartedTimestamp;

  Integer lastRunningTime;

  Integer lastTotalRecords;

  String status;

  Integer totalDeleted;

  Integer totalRecords;

  public String getError() {
    return error;
  }

  public void setError(String error) {
    this.error = error;
  }

  public LocalDateTime getLastActiveTimestamp() {
    return lastActiveTimestamp;
  }

  public void setLastActiveTimestamp(LocalDateTime lastActiveTimestamp) {
    this.lastActiveTimestamp = lastActiveTimestamp;
  }

  public Integer getLastRecsPerSec() {
    return lastRecsPerSec;
  }

  public void setLastRecsPerSec(Integer lastRecsPerSec) {
    this.lastRecsPerSec = lastRecsPerSec;
  }

  public LocalDateTime getLastStartedTimestamp() {
    return lastStartedTimestamp;
  }

  public void setLastStartedTimestamp(LocalDateTime lastStartedTimestamp) {
    this.lastStartedTimestamp = lastStartedTimestamp;
  }

  public Integer getLastRunningTime() {
    return lastRunningTime;
  }

  public void setLastRunningTime(Integer lastRunningTime) {
    this.lastRunningTime = lastRunningTime;
  }

  public Integer getLastTotalRecords() {
    return lastTotalRecords;
  }

  public void setLastTotalRecords(Integer lastTotalRecords) {
    this.lastTotalRecords = lastTotalRecords;
  }

  public String getStatus() {
    return status;
  }

  public void setStatus(String status) {
    this.status = status;
  }

  public Integer getTotalDeleted() {
    return totalDeleted;
  }

  public void setTotalDeleted(Integer totalDeleted) {
    this.totalDeleted = totalDeleted;
  }

  public Integer getTotalRecords() {
    return totalRecords;
  }

  public void setTotalRecords(Integer totalRecords) {
    this.totalRecords = totalRecords;
  }
}
