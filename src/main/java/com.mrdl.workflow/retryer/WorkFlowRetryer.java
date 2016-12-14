package com.mrdl.workflow.retryer;


import com.github.rholder.retry.Retryer;
import com.mrdl.workflow.stage.StateExecutionResponse;

public interface WorkFlowRetryer {

    Retryer<StateExecutionResponse> getRetryer();
}
