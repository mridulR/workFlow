package com.mrdl.workflow.retryer;


import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import com.mrdl.workflow.exception.RetryIngestionException;
import com.mrdl.workflow.stage.StateExecutionResponse;

import java.util.concurrent.TimeUnit;

public class DefaultWorkFlowRetryer implements WorkFlowRetryer {

  private int multiplier;
  private int maximumTime;
  private int attemptNumber;

  public DefaultWorkFlowRetryer(int multiplier, int attemptNumber, int maximumTime) {
    this.multiplier = multiplier;
    this.attemptNumber = attemptNumber;
    this.maximumTime = maximumTime;
  }

  @Override
  public Retryer<StateExecutionResponse> getRetryer() {
    return RetryerBuilder.<StateExecutionResponse>newBuilder()
        .retryIfExceptionOfType(RetryIngestionException.class)
        .withWaitStrategy(WaitStrategies.fibonacciWait(multiplier, maximumTime, TimeUnit.MINUTES))
        .withStopStrategy(StopStrategies.stopAfterAttempt(attemptNumber))
        .build();
  }

  //For testing
  void setMultiplier(int multiplier) {
    this.multiplier = multiplier;
  }

  void setAttemptNumber(int attemptNumber) {
    this.attemptNumber = attemptNumber;
  }

  void setMaximumTime(int maximumTime) {
    this.maximumTime = maximumTime;
  }
}
