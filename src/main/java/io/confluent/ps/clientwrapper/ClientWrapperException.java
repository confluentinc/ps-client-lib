package io.confluent.ps.clientwrapper;

public class ClientWrapperException extends Exception {

  public ClientWrapperException() {
    super();
  }

  public ClientWrapperException(String message) {
    super(message);
  }

  public ClientWrapperException(String message, Throwable cause) {
    super(message, cause);
  }

  public ClientWrapperException(Throwable cause) {
    super(cause);
  }

  public ClientWrapperException(String message, Throwable cause, boolean enableSuppression,
      boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
