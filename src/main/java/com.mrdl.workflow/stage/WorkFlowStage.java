package com.mrdl.workflow.stage;


import org.joda.time.DateTime;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public abstract class WorkFlowStage {

    private Map<String, StateExecutionStatus> executionStatusMap = new LinkedHashMap<>();

    public abstract StateExecutionResponse stage();

    public abstract StateExecutionResponse preProcess();

    public abstract StateExecutionResponse validate();

    public abstract StateExecutionResponse persist();

    // For testing not making it final
    public Map<String, StateExecutionStatus> getExecutionStatusMap() {
        return Collections.unmodifiableMap(executionStatusMap);
    }

    void setStartTimeForStageIfUnset(String stage, DateTime startTime) {
        StateExecutionStatus executionStatus = new StateExecutionStatus();
        if (executionStatusMap.containsKey(stage)) {
            executionStatus = executionStatusMap.get(stage);
        }
        if (executionStatus.getStartTime() == null) {
            executionStatus.setStartTime(startTime);
            executionStatusMap.put(stage, executionStatus);
        }
    }

    void setEndTimeForStageIfUnset(String stage, DateTime endTime) {
        StateExecutionStatus executionStatus = new StateExecutionStatus();
        if (executionStatusMap.containsKey(stage)) {
            executionStatus = executionStatusMap.get(stage);
        }
        if (executionStatus.getEndTime() == null) {
            executionStatus.setEndTime(endTime);
            executionStatusMap.put(stage, executionStatus);
        }
    }

    void updateStatusForStage(String stage, Status status) {
        StateExecutionStatus executionStatus = new StateExecutionStatus();
        if (executionStatusMap.containsKey(stage)) {
            executionStatus = executionStatusMap.get(stage);
        }
        executionStatus.setStatus(status);
        executionStatusMap.put(stage, executionStatus);
    }

    void incrementAttempt(String stage) {
        StateExecutionStatus executionStatus = new StateExecutionStatus();
        int currentAttempt = 0;
        if (executionStatusMap.containsKey(stage)) {
            executionStatus = executionStatusMap.get(stage);
            currentAttempt = executionStatus.getAttempt();
        }
        executionStatus.setAttempt(++currentAttempt);
        executionStatusMap.put(stage, executionStatus);
    }

    void updateStateLoadResponse(String stage, StateExecutionResponse stateExecutionResponse) {
        StateExecutionStatus executionStatus = new StateExecutionStatus();
        if (executionStatusMap.containsKey(stage)) {
            executionStatus = executionStatusMap.get(stage);
        }
        executionStatus.setStateExecutionResponse(stateExecutionResponse);
        executionStatusMap.put(stage, executionStatus);
    }

    DateTime getStartTimeForState(String stage) {
        StateExecutionStatus executionStatus = executionStatusMap.get(stage);
        if (executionStatus != null) {
            return executionStatus.getStartTime();
        }
        return null;
    }

    DateTime getEndTimeForState(String stage) {
        StateExecutionStatus executionStatus = executionStatusMap.get(stage);
        if (executionStatus != null) {
            return executionStatus.getEndTime();
        }
        return null;
    }

    int getAttemptForState(String stage) {
        StateExecutionStatus executionStatus = executionStatusMap.get(stage);
        if (executionStatus != null) {
            return executionStatus.getAttempt();
        }
        return -1;
    }

    Status getStatusForState(String stage) {
        StateExecutionStatus executionStatus = executionStatusMap.get(stage);
        if (executionStatus != null) {
            return executionStatus.getStatus();
        }
        return null;
    }

    StateExecutionResponse getStateLoadResponse(String stage) {
        StateExecutionStatus executionStatus = executionStatusMap.get(stage);
        if (executionStatus != null) {
            return executionStatus.getStateExecutionResponse();
        }
        return null;
    }
}