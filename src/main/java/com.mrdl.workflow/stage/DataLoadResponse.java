package com.mrdl.workflow.stage;

import com.mrdl.workflow.engine.StateExecutionStatus;

import java.util.HashMap;
import java.util.Map;

public final class DataLoadResponse {

  private String dataLoadBatchId;
  private Status status;
  private Map<String, StateExecutionStatus> stateLoadResponseMap;

  private DataLoadResponse(Builder builder) {
    this.dataLoadBatchId = builder.dataLoadBatchId;
    this.status = builder.status;
    this.stateLoadResponseMap = builder.stateLoadResponseMap;
  }

  public Status getStatus() {
    return status;
  }

  public Map<String, StateExecutionStatus> getStateLoadResponseMap() {
    return stateLoadResponseMap;
  }

  public String getDataLoadBatchId() {
    return dataLoadBatchId;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {

    private String dataLoadBatchId;
    private Status status;
    private Map<String, StateExecutionStatus> stateLoadResponseMap = new HashMap<>();

    public  Builder withDataLoadBatchId(String batchId) {
      this.dataLoadBatchId = batchId;
      return this;
    }

    public Builder withStatus(Status status) {
      this.status = status;
      return this;
    }

    public Builder withStateStatus(String stateName, StateExecutionStatus stateStatus) {
      stateLoadResponseMap.put(stateName, stateStatus);
      return this;
    }

    public DataLoadResponse build() {
      return new DataLoadResponse(this);
    }
  }
}
