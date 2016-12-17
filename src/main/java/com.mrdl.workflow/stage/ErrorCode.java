package com.mrdl.workflow.stage;

public enum ErrorCode {
  FILE_NOT_FOUND("File_Not_Found"),
  ACCESS_DENIED("Access_Denied"),
  INVALID_HEADERS("Invalid_Headers"),
  NULL_VALUES("Null_Values"),
  RETRY_EXCEPTION("Retry_Exception"),
  OTHER("Some other exception");

  private String description;

  ErrorCode(String description) {
    this.description = description;
  }
}
