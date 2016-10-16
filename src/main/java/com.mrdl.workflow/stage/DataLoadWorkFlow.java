package com.mrdl.workflow.stage;


import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.Retryer;

import com.mrdl.workflow.DataIngestionRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

public abstract class DataLoadWorkFlow {

  private static final Logger LOGGER = LoggerFactory.getLogger(DataLoadWorkFlow.class);

  private DataIngestionRequest dataIngestionRequest;

  public final DataIngestionRequest getDataIngestionRequest() {
    return dataIngestionRequest;
  }

  public abstract DataLoadRetryer getDataLoadRetryer();

  public abstract WorkFlowStage getWorkFlowStage();

  public final DataLoadResponse run(DataIngestionRequest dataIngestionRequest) {
    setDataIngestionRequest(dataIngestionRequest);
    final WorkFlowStage workFlowStage = getWorkFlowStage();

    try {
      Retryer<StateExecutionResponse> retryer = getDataLoadRetryer().getRetryer();
      retryer.call(getStageCallable(workFlowStage));
      retryer.call(getPreProcessCallable(workFlowStage));
      retryer.call(getValidateCallable(workFlowStage));
      retryer.call(getPersistCallable(workFlowStage));
    } catch (ExecutionException | RetryException ex) {
      LOGGER.error(
          "Exception occurred while ingesting data with batchId {}.", getDataIngestionRequest().getDataBatchId(), ex);
      return buildDataLoadErrorResponse(workFlowStage.getExecutionStatusMap());
    }
    return buildDataLoadResponse(workFlowStage.getExecutionStatusMap());
  }

  private DataLoadResponse buildDataLoadErrorResponse(Map<String, StateExecutionStatus> statusMap) {
    DataLoadResponse.Builder builder = DataLoadResponse.builder()
        .withDataLoadBatchId(getDataIngestionRequest().getDataBatchId())
        .withStatus(Status.ABORTED);
    for (Map.Entry<String, StateExecutionStatus> entry : statusMap.entrySet()) {
      builder.withStateStatus(entry.getKey(), entry.getValue());
    }
    return builder.build();
  }

  private Callable<StateExecutionResponse> getPersistCallable(final WorkFlowStage workFlowStage) {
    return new Callable<StateExecutionResponse>() {
      @Override
      public StateExecutionResponse call() throws Exception {
        return workFlowStage.persist();
      }
    };
  }

  private Callable<StateExecutionResponse> getValidateCallable(final WorkFlowStage workFlowStage) {
    return new Callable<StateExecutionResponse>() {
      @Override
      public StateExecutionResponse call() throws Exception {
        return workFlowStage.validate();
      }
    };
  }

  private Callable<StateExecutionResponse> getPreProcessCallable(final WorkFlowStage workFlowStage) {
    return new Callable<StateExecutionResponse>() {
      @Override
      public StateExecutionResponse call() throws Exception {
        return workFlowStage.preProcess();
      }
    };
  }

  private Callable<StateExecutionResponse> getStageCallable(final WorkFlowStage workFlowStage) {
    return new Callable<StateExecutionResponse>() {
      @Override
      public StateExecutionResponse call() throws Exception {
        return workFlowStage.stage();
      }
    };
  }

  private void setDataIngestionRequest(DataIngestionRequest dataIngestionRequest) {
    this.dataIngestionRequest = dataIngestionRequest;
  }

  private DataLoadResponse buildDataLoadResponse(Map<String, StateExecutionStatus> statusMap) {
    Status status = Status.OK;
    DataLoadResponse.Builder builder = DataLoadResponse.builder()
        .withDataLoadBatchId(dataIngestionRequest.getDataBatchId());
    for (Map.Entry<String, StateExecutionStatus> entry : statusMap.entrySet()) {
      builder.withStateStatus(entry.getKey(), entry.getValue());
      if (entry.getValue().getStatus() != Status.OK) {
        status = Status.FAILED;
        break;
      }
    }
    builder.withStatus(status);
    return builder.build();
  }
}
