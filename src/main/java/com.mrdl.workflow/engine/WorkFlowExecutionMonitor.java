package com.mrdl.workflow.engine;

import com.mrdl.workflow.exception.RetryIngestionException;
import com.mrdl.workflow.stage.ErrorCode;
import com.mrdl.workflow.stage.StateExecutionResponse;
import com.mrdl.workflow.stage.Status;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;

import org.joda.time.DateTime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Aspect
@Component
public class WorkFlowExecutionMonitor {

  private static final Logger LOGGER = LoggerFactory.getLogger(WorkFlowExecutionMonitor.class);

  @Before("execution(* com.mrdl.workflow.engine.WorkFlowStage.call(..)) ")
  public void recordingStartTimeForStage(JoinPoint joinPoint) {
    String methodName = joinPoint.getSignature().getName();
    DateTime startTime = DateTime.now();
    LOGGER.info("Started {} at {}", methodName, startTime.toString());
    if (joinPoint.getTarget() instanceof AbstractWorkFlowStage) {
      AbstractWorkFlowStage abstractWorkFlowStage = (AbstractWorkFlowStage) joinPoint.getTarget();
      abstractWorkFlowStage.setStartTimeForStageIfUnset(startTime);
      abstractWorkFlowStage.incrementAttempt();
      abstractWorkFlowStage.updateStatusForStage(Status.IN_PROGRESS);
    }
  }

  @Around("execution(* com.mrdl.workflow.engine.WorkFlowStage.call(..)) ")
  public void recordingStatusForStage(ProceedingJoinPoint proceedingJoinPoint) throws Throwable {
    String methodName = proceedingJoinPoint.getSignature().getName();
    DateTime startTime = DateTime.now();
    LOGGER.info("Running {} at {}", methodName, startTime.toString());
    try {
      StateExecutionResponse stateExecutionResponse = (StateExecutionResponse) proceedingJoinPoint.proceed();
      if (proceedingJoinPoint.getTarget() instanceof AbstractWorkFlowStage) {
        AbstractWorkFlowStage abstractWorkFlowStage = (AbstractWorkFlowStage) proceedingJoinPoint.getTarget();
        abstractWorkFlowStage.updateStatusForStage(Status.OK);
        abstractWorkFlowStage.updateStateLoadResponse(stateExecutionResponse);
      }
    } catch (RetryIngestionException ex) {
      if (proceedingJoinPoint.getTarget() instanceof AbstractWorkFlowStage) {
        AbstractWorkFlowStage abstractWorkFlowStage = (AbstractWorkFlowStage) proceedingJoinPoint.getTarget();
        abstractWorkFlowStage.updateStatusForStage(Status.FAILED);
        abstractWorkFlowStage.updateStateLoadResponse(StateExecutionResponse.builder()
            .isSuccessful(false).withError(ErrorCode.RETRY_EXCEPTION, ex.getMessage()).build());
        throw ex;
      }
    }
  }

  @After("execution(* com.mrdl.workflow.engine.WorkFlowStage.call(..)) ")
  public void recordingEndTimeForStage(JoinPoint joinPoint) {
    String methodName = joinPoint.getSignature().getName();
    DateTime endTime = DateTime.now();
    LOGGER.info("Completed {} at {}", methodName, endTime.toString());
    if (joinPoint.getTarget() instanceof AbstractWorkFlowStage) {
      AbstractWorkFlowStage abstractWorkFlowStage = (AbstractWorkFlowStage) joinPoint.getTarget();
      abstractWorkFlowStage.setEndTimeForStageIfUnset(endTime);
    }
  }
}
