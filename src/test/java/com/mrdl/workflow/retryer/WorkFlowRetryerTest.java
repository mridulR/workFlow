package com.mrdl.workflow.retryer;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.Retryer;

import com.mrdl.workflow.stage.RetryIngestionException;
import com.mrdl.workflow.stage.StateExecutionResponse;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.concurrent.Callable;

public class WorkFlowRetryerTest {

  private static final int MULTIPLIER = 100;
  private static final int ATTEMPT_NUMBER = 3;
  private static final int MAXIMUM_TIME = 2;

  // CSOFF: Visibility Modifier
  @Rule
  public ExpectedException expectedException = ExpectedException.none();
  // CSON: Visibility Modifier

  private WorkFlowRetryer workFlowRetryer = new WorkFlowRetryer(MULTIPLIER, ATTEMPT_NUMBER, MAXIMUM_TIME);


  @Test
  public void getRetryer_whenMethodInvoked_thenReturnRetryer() {
    Retryer<StateExecutionResponse> retryer = workFlowRetryer.getRetryer();

    assertThat(retryer != null, is(true));
  }

  @Test
  public void getStageCallable_WhenRetiesExceedsLimit_thenThrowExceptionOnExecution() throws Exception {
    expectedException.expect(RetryException.class);
    expectedException.expectMessage("Retrying failed to complete successfully after 2 attempts.");

    Callable<StateExecutionResponse> mockStageCallable = mock(Callable.class);
    when(mockStageCallable.call())
        .thenThrow(new RetryIngestionException("FirstTime"))
        .thenThrow(new RetryIngestionException("SecondTime"))
        .thenThrow(new RetryIngestionException("ThirdTime"));
    workFlowRetryer.setAttemptNumber(2);

    workFlowRetryer.getRetryer().call(mockStageCallable);
  }
}