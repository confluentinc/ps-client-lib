package io.confluent.ps.clientwrapper;

public class DeprecatedException extends ClientWrapperRuntimeException {

  public DeprecatedException() {
    super("Deprecated");
  }
}
