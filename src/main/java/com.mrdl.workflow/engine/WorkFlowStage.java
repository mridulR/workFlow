package com.mrdl.workflow.engine;

import com.mrdl.workflow.stage.StateExecutionResponse;

import java.util.concurrent.Callable;

public interface WorkFlowStage extends Callable<StateExecutionResponse> {

  String getStageName();

  StateExecutionStatus getStateExecutionStatus();
}
