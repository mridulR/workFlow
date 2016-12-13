package com.mrdl.workflow;

public class WorkFlowContext {

  private String dataBatchId;
  private Location fileLocation;

  public WorkFlowContext(Builder builder) {
    this.dataBatchId = builder.dataBatchId;
    this.fileLocation = builder.fileLocation;
  }

  public static Builder builder() {
    return new Builder();
  }

  public String getDataBatchId() {
    return dataBatchId;
  }

  public Location getFileLocation() {
    return fileLocation;
  }

  public static final class Builder {

    private String dataBatchId;
    private Location fileLocation;

    public Builder() {}

    public Builder withDataBatchId(String val) {
      dataBatchId = val;
      return this;
    }

    public Builder withFileLocation(Location fileLocation) {
      this.fileLocation = fileLocation;
      return this;
    }

    public WorkFlowContext build() {
      return new WorkFlowContext(this);
    }
  }
}
