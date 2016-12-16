package com.mrdl.workflow.engine;

import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.Retryer;
import com.mrdl.workflow.WorkFlowContext;
import com.mrdl.workflow.retryer.WorkFlowRetryer;
import com.mrdl.workflow.stage.DataLoadResponse;
import com.mrdl.workflow.stage.StateExecutionResponse;
import com.mrdl.workflow.stage.Status;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public abstract class WorkFlowRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(WorkFlowRunner.class);

    private Map<String, StateExecutionStatus> executionStatusMap = new LinkedHashMap<>();

    private WorkFlowContext workFlowContext;

    public final WorkFlowContext getWorkFlowContext() {
        return workFlowContext;
    }

    public abstract WorkFlowRetryer getDataLoadRetryer();

    public abstract WorkFlow getWorkFlow();

    public final DataLoadResponse run(WorkFlowContext workFlowContext) {
        setWorkFlowContext(workFlowContext);
        final WorkFlow workFlow = getWorkFlow();
        Retryer<StateExecutionResponse> retryer = getDataLoadRetryer().getRetryer();
        for (WorkFlowStage workFlowStage : workFlow.getWorkFlowStagesInSequence()) {
            try {
                retryer.call(workFlowStage);
            } catch (ExecutionException | RetryException ex) {
                LOGGER.error("Exception occurred while ingesting data with batchId {}.",
                        getWorkFlowContext().getDataBatchId(), ex);
                return buildDataLoadErrorResponse(executionStatusMap);
            } finally {
                executionStatusMap.put(workFlowStage.getStageName(), workFlowStage.getStateExecutionStatus());
            }
        }
        return buildDataLoadResponse(executionStatusMap);
    }

    private DataLoadResponse buildDataLoadErrorResponse(Map<String, StateExecutionStatus> statusMap) {
        DataLoadResponse.Builder builder = DataLoadResponse.builder()
                .withDataLoadBatchId(getWorkFlowContext().getDataBatchId())
                .withStatus(Status.ABORTED);
        for (Map.Entry<String, StateExecutionStatus> entry : statusMap.entrySet()) {
            builder.withStateStatus(entry.getKey(), entry.getValue());
        }
        return builder.build();
    }

    private void setWorkFlowContext(WorkFlowContext workFlowContext) {
        this.workFlowContext = workFlowContext;
    }

    private DataLoadResponse buildDataLoadResponse(Map<String, StateExecutionStatus> statusMap) {
        Status status = Status.OK;
        DataLoadResponse.Builder builder = DataLoadResponse.builder()
                .withDataLoadBatchId(workFlowContext.getDataBatchId());
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