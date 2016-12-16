package com.mrdl.workflow.engine;

import com.mrdl.workflow.stage.StateExecutionResponse;
import com.mrdl.workflow.stage.Status;
import org.joda.time.DateTime;

public final class StateExecutionStatus {

    private DateTime startTime;
    private DateTime endTime;
    private Status status;
    private int attempt;
    private StateExecutionResponse stateExecutionResponse;

    public DateTime getStartTime() {
        return startTime;
    }

    public int getAttempt() {
        return attempt;
    }

    public Status getStatus() {
        return status;
    }

    public DateTime getEndTime() {
        return endTime;
    }

    void setStartTime(DateTime startTime) {
        this.startTime = startTime;
    }

    void setEndTime(DateTime endTime) {
        this.endTime = endTime;
    }

    void setStatus(Status status) {
        this.status = status;
    }

    void setAttempt(int attempt) {
        this.attempt = attempt;
    }

    public StateExecutionResponse getStateExecutionResponse() {
        return stateExecutionResponse;
    }

    public void setStateExecutionResponse(StateExecutionResponse stateExecutionResponse) {
        this.stateExecutionResponse = stateExecutionResponse;
    }
}
