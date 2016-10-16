package com.mrdl.workflow.stage;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class StateExecutionResponseTest {

  // CSOFF: Visibility Modifier
  @Rule
  public ExpectedException expectedException = ExpectedException.none();
  // CSON: Visibility Modifier

  @Test
  public void build_whenStatusIsSuccess_thenErrorListShouldBeEmpty() {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("When state load status is true, then errorCodes must be empty");

    StateExecutionResponse.builder().isSuccessful(true).withError(ErrorCode.FILE_NOT_FOUND, "Not_Found").build();
  }

  @Test
  public void build_whenStatusIsFalse_thenErrorListShouldBeNonEmpty() {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("When state load status is false, then errorCodes must not be empty");

    StateExecutionResponse.builder().isSuccessful(false).build();
  }



}