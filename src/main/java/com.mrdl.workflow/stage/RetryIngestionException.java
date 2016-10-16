package com.mrdl.workflow.stage;

public class RetryIngestionException extends RuntimeException {

  public RetryIngestionException(String message, Throwable cause) {
    super(message, cause);
  }

  public RetryIngestionException(String message) {
    super(message);
  }
}
