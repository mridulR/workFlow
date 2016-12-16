package com.mrdl.workflow.engine;


import com.mrdl.workflow.stage.StateExecutionResponse;
import com.mrdl.workflow.stage.Status;
import org.joda.time.DateTime;

public abstract class AbstractWorkFlowStage implements WorkFlowStage {

    private StateExecutionStatus stateExecutionStatus = new StateExecutionStatus();

    public StateExecutionStatus getStateExecutionStatus() {
        return stateExecutionStatus;
    }

    void setStartTimeForStageIfUnset(DateTime startTime) {
        if (stateExecutionStatus.getStartTime() == null) {
            stateExecutionStatus.setStartTime(startTime);
        }
    }

    void incrementAttempt() {
        int currentAttempt = stateExecutionStatus.getAttempt();
        stateExecutionStatus.setAttempt(++currentAttempt);
    }

    void updateStatusForStage(Status status) {
        stateExecutionStatus.setStatus(status);
    }

    void updateStateLoadResponse(StateExecutionResponse stateExecutionResponse) {
        stateExecutionStatus.setStateExecutionResponse(stateExecutionResponse);
    }

    void setEndTimeForStageIfUnset(DateTime endTime) {
        stateExecutionStatus.setEndTime(endTime);
    }
}
