package com.mrdl.workflow.stage;

public enum Status {
  OK("Ok"), IN_PROGRESS("In_Progress"), FAILED("Failed"), ABORTED("Aborted");

  private String name;

  Status(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }
}
