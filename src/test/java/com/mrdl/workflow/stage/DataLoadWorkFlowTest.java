package com.mrdl.workflow.stage;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;

import com.mrdl.workflow.WorkFlowContext;
import org.junit.Before;
import org.junit.Test;
import org.springframework.aop.aspectj.annotation.AspectJProxyFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

// CSOFF: MagicNumber
public class DataLoadWorkFlowTest {

  private static final String BATCH_ID = "batchId";

  private DataLoadWorkFlow dataLoadWorkFlow;

  private WorkFlowContext workFlowContext;
  private DataLoadRetryer dataLoadRetryer;
  private WorkFlowStage workFlowStage;

  @Before
  public void setUp() {
    workFlowContext = mock(WorkFlowContext.class);
    when(workFlowContext.getDataBatchId()).thenReturn(BATCH_ID);
    dataLoadRetryer = mock(DataLoadRetryer.class);
    dataLoadWorkFlow = new DataLoadWorkFlow() {
      @Override
      public DataLoadRetryer getDataLoadRetryer() {
        return dataLoadRetryer;
      }

      @Override
      public WorkFlowStage getWorkFlowStage() {
        return workFlowStageAspectAwareProxy(workFlowStage);
      }
    };

    Retryer<StateExecutionResponse> retryer = RetryerBuilder.<StateExecutionResponse>newBuilder()
        .retryIfExceptionOfType(RetryIngestionException.class)
        .withWaitStrategy(WaitStrategies.fibonacciWait(100, 3, TimeUnit.MINUTES))
        .withStopStrategy(StopStrategies.stopAfterAttempt(3))
        .build();
    when(dataLoadRetryer.getRetryer()).thenReturn(retryer);
  }

  @Test
  public void run_WhenAllStagesRunSuccessfully_thenReturnSuccessDataLoadResponse() throws Exception {

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

    DataLoadResponse response = dataLoadWorkFlow.run(workFlowContext);

    assertThat(response.getDataLoadBatchId(), is(BATCH_ID));
    assertThat(response.getStatus(), is(Status.OK));
    assertThat(response.getStateLoadResponseMap().size(), is(4));
  }

  private WorkFlowStage workFlowStageAspectAwareProxy(WorkFlowStage workFlowStage) {
    AspectJProxyFactory factory = new AspectJProxyFactory(workFlowStage);
    DataLoaderExecutionMonitor aspect = new DataLoaderExecutionMonitor();
    factory.addAspect(aspect);
    return factory.getProxy();
  }

  @Test
  public void run_WhenSomeStageFailWithRetryExhaustException_thenReturnAbortedDataLoadResponse() throws Exception {

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
        throw new RetryExhaustIngestionException("Retry exhaust");
      }

      @Override
      public StateExecutionResponse persist() {
        return StateExecutionResponse.builder().isSuccessful(true).build();
      }
    };

    DataLoadResponse response = dataLoadWorkFlow.run(workFlowContext);

    assertThat(response.getDataLoadBatchId(), is(BATCH_ID));
    assertThat(response.getStatus(), is(Status.ABORTED));
    assertThat(response.getStateLoadResponseMap().size(), is(3));
    assertThat(response.getStateLoadResponseMap().get("validate").getAttempt(), is(1));
  }

  @Test
  public void run_WhenSomeStageFailWithRetryExceptionContinuously_thenReturnAbortedDataLoadResponse() throws Exception {

    workFlowStage = new WorkFlowStage() {

      private final AtomicInteger count = new AtomicInteger();

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
        throw new RetryIngestionException("Ran " + count.incrementAndGet() + " times");
      }

      @Override
      public StateExecutionResponse persist() {
        return StateExecutionResponse.builder().isSuccessful(true).build();
      }
    };

    DataLoadResponse response = dataLoadWorkFlow.run(workFlowContext);

    assertThat(response.getDataLoadBatchId(), is(BATCH_ID));
    assertThat(response.getStatus(), is(Status.ABORTED));
    assertThat(response.getStateLoadResponseMap().size(), is(3));
    assertThat(response.getStateLoadResponseMap().get("validate").getStateExecutionResponse().isSuccessful(),
               is(false));
    assertThat(response.getStateLoadResponseMap().get("validate").getStateExecutionResponse().getErrorCodes()
                   .get(ErrorCode.RETRY_EXCEPTION), is("Ran 3 times"));
    assertThat(response.getStateLoadResponseMap().get("validate").getAttempt(), is(3));
  }

  @Test
  public void run_WhenSomeStageFailWithRetryException_thenReturnSuccessDataLoadResponseAfterRetry() throws Exception {

    workFlowStage = new WorkFlowStage() {

      private final AtomicInteger count = new AtomicInteger();

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
        if (count.incrementAndGet() < 3) {
          throw new RetryIngestionException("Ran " + count.get() + " times");
        }
        return StateExecutionResponse.builder().isSuccessful(true).build();
      }

      @Override
      public StateExecutionResponse persist() {
        return StateExecutionResponse.builder().isSuccessful(true).build();
      }
    };

    DataLoadResponse response = dataLoadWorkFlow.run(workFlowContext);

    assertThat(response.getDataLoadBatchId(), is(BATCH_ID));
    assertThat(response.getStatus(), is(Status.OK));
    assertThat(response.getStateLoadResponseMap().size(), is(4));
    assertThat(response.getStateLoadResponseMap().get("validate").getStateExecutionResponse().isSuccessful(),
               is(true));
    assertThat(response.getStateLoadResponseMap().get("validate").getAttempt(), is(3));
  }
}