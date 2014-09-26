package com.intel.picklepot.exception;

public class PicklePotException extends Exception {

  public PicklePotException() {
  }

  public PicklePotException(String msg) {
    super(msg);
  }

  public PicklePotException(Throwable e) {
    super(e);
  }

  public PicklePotException(String msg, Throwable e) {
    super(msg, e);
  }
}
