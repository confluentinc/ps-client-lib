package io.confluent.is.clientwrapper;

public class ClientWrapperException extends RuntimeException {

  public ClientWrapperException(Exception e) {
    super(e);
  }
}
