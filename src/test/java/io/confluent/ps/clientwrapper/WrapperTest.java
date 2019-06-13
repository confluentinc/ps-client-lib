package io.confluent.ps.clientwrapper;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.awaitility.Awaitility.await;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import static org.hamcrest.Matchers.*;

public class WrapperTest {

  @Before
  public void setup(){
    System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "INFO");
    System.setProperty("org.slf4j.simpleLogger.log.io.confluent.ps", "TRACE");

    final org.slf4j.Logger log = LoggerFactory.getLogger(WrapperTest.class);

    // Validate logging config
    log.trace("trace");
    log.debug("debug");
    log.info("info");
    log.warn("warning");
    log.error("error");
  }

  @Test
  public void BasicTest() throws InterruptedException, ExecutionException, TimeoutException {
    ClientWrapper cw = new ClientWrapper();
    while(true) {
      sendConfigMessage(cw);
      Thread.sleep(2000);
    }

//    await().atMost(5, MINUTES).until(() -> false, is(true));
  }

  private void sendConfigMessage(ClientWrapper cw) {
    cw.getWrappedProducer().send(new ProducerRecord(ClientWrapper.CLIENT_CONFIG_TOPIC, ClientWrapper.APPLICATION_NAME, "a-config-" + Math.random()));
  }

}
