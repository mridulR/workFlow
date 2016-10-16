package com.mrdl.workflow.stage;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.HashMap;
import java.util.Map;

public final class StateExecutionResponse {

  private boolean isSuccessful;
  private Map<ErrorCode, String> errorCodes;

  private StateExecutionResponse(Builder builder) {
    this.isSuccessful = builder.isSuccessful;
    this.errorCodes = builder.errorCodes;
  }

  public boolean isSuccessful() {
    return isSuccessful;
  }

  public Map<ErrorCode, String> getErrorCodes() {
    return errorCodes;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {

    private boolean isSuccessful;
    private Map<ErrorCode, String> errorCodes = new HashMap<>();

    public Builder isSuccessful(boolean status) {
      isSuccessful = status;
      return this;
    }

    public Builder withError(ErrorCode errorCode, String errorDescription) {
      errorCodes.put(errorCode, errorDescription);
      return this;
    }

    public StateExecutionResponse build() {
      if (isSuccessful) {
        checkArgument(errorCodes.isEmpty(), "When state load status is true, then errorCodes must be empty");
      } else {
        checkArgument(!errorCodes.isEmpty(), "When state load status is false, then errorCodes must not be empty");
      }
      return new StateExecutionResponse(this);
    }
  }
}
