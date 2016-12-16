package com.mrdl.workflow;

import org.apache.commons.lang3.RandomStringUtils;

public class WorkFlowContext {

  private String batchId = RandomStringUtils.randomAlphanumeric(5);

  public String getBatchId() {
    return batchId;
  }

  public void setBatchId(String batchId) {
    this.batchId = batchId;
  }
}
