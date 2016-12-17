package com.mrdl.workflow.engine;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

import com.mrdl.workflow.stage.StateExecutionResponse;
import com.mrdl.workflow.stage.Status;

import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;

public class AbstractWorkFlowStageTest {

  private static final String STAGE_NAME = "stageName";
  private static final StateExecutionResponse  DEFAULT_STATE_EXECUTION_RESPONSE = StateExecutionResponse
      .builder().isSuccessful(true).build();

  private AbstractWorkFlowStage abstractWorkFlowStage;

  @Before
  public void setUp() {
    abstractWorkFlowStage = new AbstractWorkFlowStage() {
      @Override
      public String getStageName() {
        return STAGE_NAME;
      }

      @Override
      public StateExecutionResponse call() throws Exception {
        return DEFAULT_STATE_EXECUTION_RESPONSE;
      }
    };
  }

  @Test
  public void getStateExecutionStatus_givenStateExecutionStatusIsNotIntercepted_thenReturnDefaultState() {
    StateExecutionStatus stateExecutionStatus = abstractWorkFlowStage.getStateExecutionStatus();

    assertThat(stateExecutionStatus.getAttempt(), is(0));
    assert(stateExecutionStatus.getEndTime() == null);
    assert(stateExecutionStatus.getStartTime() == null);
    assert(stateExecutionStatus.getStateExecutionResponse() == null);
    assert(stateExecutionStatus.getStatus() == null);
  }

  @Test
  public void getStateExecutionStatus_givenRetried_thenIncrementAttempt() {
    abstractWorkFlowStage.incrementAttempt();
    StateExecutionStatus stateExecutionStatus = abstractWorkFlowStage.getStateExecutionStatus();

    assertThat(stateExecutionStatus.getAttempt(), is(1));
  }

  @Test
  public void getStateExecutionStatus_whenStatusIsSet_thenReturnStatus() {
    abstractWorkFlowStage.updateStatusForStage(Status.IN_PROGRESS);
    StateExecutionStatus stateExecutionStatus = abstractWorkFlowStage.getStateExecutionStatus();

    assertThat(stateExecutionStatus.getStatus(), is(Status.IN_PROGRESS));
  }

  @Test
  public void getStateExecutionStatus_whenStartTimeIsSet_thenReturnStartTime() {
    abstractWorkFlowStage.setStartTimeForStageIfUnset(DateTime.now());
    StateExecutionStatus stateExecutionStatus = abstractWorkFlowStage.getStateExecutionStatus();

    assert(stateExecutionStatus.getStartTime() != null);
  }

  @Test
  public void getStateExecutionStatus_whenEndTimeIsSet_thenReturnEndTime() {
    abstractWorkFlowStage.setEndTimeForStageIfUnset(DateTime.now());
    StateExecutionStatus stateExecutionStatus = abstractWorkFlowStage.getStateExecutionStatus();

    assert(stateExecutionStatus.getEndTime() != null);
  }

  @Test
  public void getStateExecutionStatus_whenStateResponseIsSet_thenReturnResponse() {
    abstractWorkFlowStage.updateStateLoadResponse(DEFAULT_STATE_EXECUTION_RESPONSE);
    StateExecutionStatus stateExecutionStatus = abstractWorkFlowStage.getStateExecutionStatus();

    assertThat(stateExecutionStatus.getStateExecutionResponse(), is(DEFAULT_STATE_EXECUTION_RESPONSE));
  }
}