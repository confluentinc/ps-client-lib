package io.confluent.ps.clientwrapper;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.commons.lang.BooleanUtils.isFalse;
import static org.awaitility.Awaitility.await;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.awaitility.Duration;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

public class WrapperTest {

  public static final int PAUSE_TIME = 1; // a single ms seems to be sufficient for the code to block as expected

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
  public void basicTest() throws InterruptedException, ExecutionException, TimeoutException {
    ClientWrapper cw = new ClientWrapper();
//    while(true) {
      sendConfigMessage(cw);
//      Thread.sleep(2000);
//    }

    await().atMost(5, MINUTES).until(() -> false, is(true));
  }

  private void sendConfigMessage(ClientWrapper cw) throws ExecutionException, InterruptedException {
    cw.getWrappedProducer().send(new ProducerRecord(ClientWrapper.CLIENT_CONFIG_TOPIC, ClientWrapper.APPLICATION_NAME, "a-config-" + Math.random())).get();
  }

  @Test
  public void testChangeAcksModeAll() throws ExecutionException, InterruptedException {
    WrappedProducer wp = new WrappedProducer(new Properties());
    wp.send(new ProducerRecord(ClientWrapper.CLIENT_CONFIG_TOPIC, ClientWrapper.APPLICATION_NAME, "acks=all")).get();
  }

  @Test
  public void testChangeAcksModeOne() throws ExecutionException, InterruptedException {
    WrappedProducer wp = new WrappedProducer(new Properties());
    wp.send(new ProducerRecord(ClientWrapper.CLIENT_CONFIG_TOPIC, ClientWrapper.APPLICATION_NAME, "acks=1")).get();
  }

  @Test
  public void testProducerLock() throws ExecutionException, InterruptedException {
    ExecutorService executorService =
        new ThreadPoolExecutor(10, 10, 0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<Runnable>());

    ClientWrapper cw = new ClientWrapper();

    WrappedProducer wrappedProducer = cw.getWrappedProducer();

    WrappedProducer oldProducer = wrappedProducer;

    // switch the delegate to the new producer first so all new requests go to the new config
    WrappedProducer newProducer = new WrappedProducer(new Properties());
    cw.wrappedProducer = newProducer;

    // try using the new producer to simulate multi threaded access during the swap
    Runnable runnable = () -> {
      newProducer.flush(); // should block
    };
    Future<?> submit = executorService.submit(runnable);
    Thread.sleep(PAUSE_TIME); // yield for a second
    assertThat("new client use is blocking", submit.isDone(), is(false)); // should block

    // try another thread
    // try using the new producer to simulate multi threaded access during the swap
    Runnable anotherRunnable = () -> {
      newProducer.flush(); // should block
    };
    Future<?> submit2 = executorService.submit(anotherRunnable);
    Thread.sleep(PAUSE_TIME); // yield for a second
    assertThat("new client use is blocking", submit2.isDone(), is(false)); // should block


    // close the old producer
    oldProducer.close();

    assertThat("new client use is blocking", executorService.submit(runnable).isDone(), is(false));

    // unlock the mutex
    newProducer.unblock();

    Runnable runnable2 = () -> {
      newProducer.flush(); // should not block
    };
    Future<?> submit1 = executorService.submit(runnable2);
    Thread.sleep(PAUSE_TIME); // yield for a second
    assertThat("new client use is no longer blocking", submit1.isDone(), is(true));

    assertThat("new client use is blocking", submit2.isDone(), is(true)); // should block
    assertThat("new client use is blocking", submit1.isDone(), is(true)); // should block
  }

}
