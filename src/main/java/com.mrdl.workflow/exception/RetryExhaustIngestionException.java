package com.mrdl.workflow.exception;

public class RetryExhaustIngestionException extends RuntimeException {

  public RetryExhaustIngestionException(Throwable cause) {
    super(cause);
  }

  public RetryExhaustIngestionException(String message) {
    super(message);
  }
}
