package io.confluent.ps.clientwrapper;

public class ClientWrapperRuntimeException extends RuntimeException {

  public ClientWrapperRuntimeException(Throwable cause) {
    super(cause);
  }

  public ClientWrapperRuntimeException(String message, Throwable cause) {
    super(message, cause);
  }

  public ClientWrapperRuntimeException(String msg) {
    super(msg);
  }
}
