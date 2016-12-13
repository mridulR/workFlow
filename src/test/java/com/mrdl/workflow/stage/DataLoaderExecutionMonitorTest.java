package com.mrdl.workflow.stage;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


import com.mrdl.workflow.WorkFlowContext;
import org.junit.Before;
import org.junit.Test;
import org.springframework.aop.aspectj.annotation.AspectJProxyFactory;

public class DataLoaderExecutionMonitorTest {

  private WorkFlowStage workFlowStage;

  @Before
  public void setUp() {
    workFlowStage = new WorkFlowStage() {
      @Override
      public StateExecutionResponse stage() {
        return StateExecutionResponse.builder().isSuccessful(true).build();
      }

      @Override
      public StateExecutionResponse preProcess() {
        return StateExecutionResponse.builder().isSuccessful(true).build();
      }

      @Override
      public StateExecutionResponse validate() {
        return StateExecutionResponse.builder().isSuccessful(true).build();
      }

      @Override
      public StateExecutionResponse persist() {
        return StateExecutionResponse.builder().isSuccessful(true).build();
      }
    };
  }

  @Test
  public void recordingStartTimeForStage_givenMethodIsCalled_thenUpdateStartTimeForState() {

    workFlowStageAspectAwareProxy(workFlowStage).stage();
    workFlowStageAspectAwareProxy(workFlowStage).preProcess();
    workFlowStageAspectAwareProxy(workFlowStage).validate();
    workFlowStageAspectAwareProxy(workFlowStage).persist();
    workFlowStageAspectAwareProxy(workFlowStage).validate();
    workFlowStageAspectAwareProxy(workFlowStage).persist();

    assertThat(workFlowStage.getStartTimeForState("stage") != null, is(true));
    assertThat(workFlowStage.getStartTimeForState("preProcess") != null, is(true));
    assertThat(workFlowStage.getStartTimeForState("validate") != null, is(true));
    assertThat(workFlowStage.getStartTimeForState("persist") != null, is(true));

    assertThat(workFlowStage.getAttemptForState("stage"), is(1));
    assertThat(workFlowStage.getAttemptForState("preProcess"), is(1));
    assertThat(workFlowStage.getAttemptForState("validate"), is(2));
    assertThat(workFlowStage.getAttemptForState("persist"), is(2));
    assertThat(workFlowStage.getAttemptForState("UnKnownStage"), is(-1));

    assertThat(workFlowStage.getStatusForState("stage"), is(Status.OK));
    assertThat(workFlowStage.getStatusForState("preProcess"), is(Status.OK));
    assertThat(workFlowStage.getStatusForState("validate"), is(Status.OK));
    assertThat(workFlowStage.getStatusForState("persist"), is(Status.OK));
  }

  private WorkFlowStage workFlowStageAspectAwareProxy(WorkFlowStage workFlowStage) {
    AspectJProxyFactory factory = new AspectJProxyFactory(workFlowStage);
    DataLoaderExecutionMonitor aspect = new DataLoaderExecutionMonitor();
    factory.addAspect(aspect);
    return factory.getProxy();
  }

  @Test
  public void recordingEndTimeForStage_givenMethodIsCalled_thenUpdateEndTimeForState() {

    workFlowStageAspectAwareProxy(workFlowStage).stage();
    workFlowStageAspectAwareProxy(workFlowStage).preProcess();
    workFlowStageAspectAwareProxy(workFlowStage).validate();
    workFlowStageAspectAwareProxy(workFlowStage).persist();

    assertThat(workFlowStage.getEndTimeForState("stage") != null, is(true));
    assertThat(workFlowStage.getEndTimeForState("preProcess") != null, is(true));
    assertThat(workFlowStage.getEndTimeForState("validate") != null, is(true));
    assertThat(workFlowStage.getEndTimeForState("persist") != null, is(true));
  }

  @Test
  public void recordingStatusForState_givenExceptionOccursWhileExecution_thenUpdateStatusAsFailed() {

    workFlowStage = new WorkFlowStage() {
      @Override
      public StateExecutionResponse stage() {
        throw new RetryIngestionException("Stage");
      }

      @Override
      public StateExecutionResponse preProcess() {
        return null;
      }

      @Override
      public StateExecutionResponse validate() {
        throw new RetryIngestionException("validate");
      }

      @Override
      public StateExecutionResponse persist() {
        return null;
      }
    };

    try {
      workFlowStageAspectAwareProxy(workFlowStage).stage();
      fail();
    } catch (RetryIngestionException ex) {
      // This is expected
    }

    workFlowStageAspectAwareProxy(workFlowStage).preProcess();

    try {
      workFlowStageAspectAwareProxy(workFlowStage).validate();
      fail();
    } catch (RetryIngestionException ex) {
      // This is expected
    }

    workFlowStageAspectAwareProxy(workFlowStage).persist();

    assertThat(workFlowStage.getStatusForState("stage"), is(Status.FAILED));
    assertThat(workFlowStage.getStatusForState("preProcess"), is(Status.OK));
    assertThat(workFlowStage.getStatusForState("validate"), is(Status.FAILED));
    assertThat(workFlowStage.getStatusForState("persist"), is(Status.OK));
  }

  //Negative test
  @Test
  public void when_getExecutionStatusMapIsCalled_TimeKeeperShouldNotBeExecuted() {
    WorkFlowContext workFlowContext = mock(WorkFlowContext.class);
    when(workFlowContext.getDataBatchId()).thenReturn("batchId");
    workFlowStageAspectAwareProxy(workFlowStage).getExecutionStatusMap();

    assertThat(workFlowStage.getAttemptForState("getExecutionStatusMap"), is(-1));
  }
}