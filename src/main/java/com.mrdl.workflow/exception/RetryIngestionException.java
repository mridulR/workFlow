package com.mrdl.workflow.exception;

public class RetryIngestionException extends RuntimeException {

  public RetryIngestionException(String message, Throwable cause) {
    super(message, cause);
  }

  public RetryIngestionException(String message) {
    super(message);
  }
}
