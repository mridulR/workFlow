package com.mrdl.workflow.stage;



import com.mrdl.workflow.exception.RetryIngestionException;
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
public class DataLoaderExecutionMonitor {

  private static final Logger LOGGER = LoggerFactory.getLogger(DataLoaderExecutionMonitor.class);

  @Before("execution(* com.mrdl.workflow.stage.WorkFlowStage+.stage(..))  "
          + "|| execution(* com.mrdl.workflow.stage.WorkFlowStage+.preProcess(..)) "
          + "|| execution(* com.mrdl.workflow.stage.WorkFlowStage+.validate(..)) "
          + "|| execution(* com.mrdl.workflow.stage.WorkFlowStage+.persist(..))")
  public void recordingStartTimeForStage(JoinPoint joinPoint) {
    String methodName = joinPoint.getSignature().getName();
    DateTime startTime = DateTime.now();
    LOGGER.info("Started {} for mfp data load at {}", methodName, startTime.toString());
    if (joinPoint.getTarget() instanceof WorkFlowStage) {
      WorkFlowStage workFlowStage = (WorkFlowStage) joinPoint.getTarget();
      workFlowStage.setStartTimeForStageIfUnset(methodName, startTime);
      workFlowStage.incrementAttempt(methodName);
      workFlowStage.updateStatusForStage(methodName, Status.IN_PROGRESS);
    }
  }

  @Around("execution(* com.mrdl.workflow.stage.WorkFlowStage+.stage(..))  "
          + "|| execution(* com.mrdl.workflow.stage.WorkFlowStage+.preProcess(..)) "
          + "|| execution(* com.mrdl.workflow.stage.WorkFlowStage+.validate(..)) "
          + "|| execution(* com.mrdl.workflow.stage.WorkFlowStage+.persist(..))")
  public void recordingStatusForStage(ProceedingJoinPoint proceedingJoinPoint) throws Throwable {
    String methodName = proceedingJoinPoint.getSignature().getName();
    DateTime startTime = DateTime.now();
    LOGGER.info("Running {} for mfp data load at {}", methodName, startTime.toString());
    try {
      StateExecutionResponse stateExecutionResponse = (StateExecutionResponse) proceedingJoinPoint.proceed();
      if (proceedingJoinPoint.getTarget() instanceof WorkFlowStage) {
        WorkFlowStage workFlowStage = (WorkFlowStage) proceedingJoinPoint.getTarget();
        workFlowStage.updateStatusForStage(methodName, Status.OK);
        workFlowStage.updateStateLoadResponse(methodName, stateExecutionResponse);
      }
    } catch (RetryIngestionException ex) {
      if (proceedingJoinPoint.getTarget() instanceof WorkFlowStage) {
        WorkFlowStage workFlowStage = (WorkFlowStage) proceedingJoinPoint.getTarget();
        workFlowStage.updateStatusForStage(methodName, Status.FAILED);
        workFlowStage.updateStateLoadResponse(methodName, StateExecutionResponse.builder()
            .isSuccessful(false).withError(ErrorCode.RETRY_EXCEPTION, ex.getMessage()).build());
        throw ex;
      }
    }
  }

  private StateExecutionResponse getDefaultStateLoadResponse() {
    return StateExecutionResponse.builder().isSuccessful(true).build();
  }

  @After("execution(* com.mrdl.workflow.stage.WorkFlowStage+.stage(..))  "
         + "|| execution(* com.mrdl.workflow.stage.WorkFlowStage+.preProcess(..)) "
         + "|| execution(* com.mrdl.workflow.stage.WorkFlowStage+.validate(..)) "
         + "|| execution(* com.mrdl.workflow.stage.WorkFlowStage+.persist(..))")
  public void recordingEndTimeForStage(JoinPoint joinPoint) {
    String methodName = joinPoint.getSignature().getName();
    DateTime endTime = DateTime.now();
    LOGGER.info("Completed {} for mfp data load at {}", methodName, endTime.toString());
    if (joinPoint.getTarget() instanceof WorkFlowStage) {
      WorkFlowStage workFlowStage = (WorkFlowStage) joinPoint.getTarget();
      workFlowStage.setEndTimeForStageIfUnset(methodName, endTime);
    }
  }
}
