package io.confluent.ps.clientwrapper;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.RandomUtils;
import static io.confluent.ps.clientwrapper.ClientWrapper.CLIENT_CONFIG_COMMANDS_TOPIC;
import static io.confluent.ps.clientwrapper.ClientWrapper.CLIENT_CONFIG_TOPIC;
import static io.confluent.ps.clientwrapper.ClientWrapper.CLIENT_META_DATA_TOPIC;
import static io.confluent.ps.clientwrapper.ClientWrapper.CLIENT_METRICS_TOPIC;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.commons.lang.math.RandomUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.*;

import static io.confluent.ps.clientwrapper.ClientWrapper.*;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class WrapperTest {

  Logger log = LoggerFactory.getLogger(WrapperTest.class);

  @Rule
//  public KafkaContainer kafka = new KafkaContainer("5.2.0");
  public KafkaContainer kafka;


  public static final int PAUSE_TIME = 1; // a single ms seems to be sufficient for the code to block as expected

  ObjectMapper mapper = new ObjectMapper();
//      .setAnnotationIntrospector(new JacksonLombokAnnotationIntrospector());

  @Before
  public void setup() {
    System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "INFO");
    System.setProperty("org.slf4j.simpleLogger.log.io.confluent.ps", "TRACE");
    System.setProperty("org.slf4j.simpleLogger.log.org.apache", "ERROR");

    final org.slf4j.Logger log = LoggerFactory.getLogger(WrapperTest.class);

    // Validate logging config
    log.trace("trace");
    log.debug("debug");
    log.info("info");
    log.warn("warning");
    log.error("error");

    ensureTopics();
  }

  private void ensureTopics() {
    Properties properties = new Properties();
    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrap());
    AdminClient adminClient = AdminClient.create(properties);
    List<String> topics = new ArrayList(
        Arrays.asList(CLIENT_CONFIG_TOPIC, CLIENT_CONFIG_COMMANDS_TOPIC, CLIENT_META_DATA_TOPIC));
    List<NewTopic> newtopics = new ArrayList<>();
    for (String topic : topics) {
      // TODO rep factor - need to fix this. Move to test
      newtopics.add(new NewTopic(topic, 3, (short) 1));
    }
    adminClient.createTopics(newtopics);
  }

  private String getBootstrap() {
    if (kafka == null) {
      return "localhost:9092";
    } else {
      return kafka.getBootstrapServers();
    }
  }

  @Test
  public void basicTest()
      throws InterruptedException, ExecutionException, TimeoutException, JsonProcessingException {
    ClientWrapper cw = new ClientWrapper(getBootstrap());
//    while(true) {
    sendConfigMessage(cw);
//      Thread.sleep(2000);
//    }

    await().atMost(5, MINUTES).until(() -> false, is(true));
  }

  private void sendConfigMessage(ClientWrapper cw)
      throws ExecutionException, InterruptedException, JsonProcessingException {
//    ObjectNode on = mapper.createObjectNode();
//    on.put("app-id", "myappid");
//    ObjectNode configs = on.putObject("configs");
//    configs.put()
//    configs.add()
//

    WrapperClientConfg c = getNewConfig();
    int value = (int) (RandomUtils.nextDouble() * 10000);
    c.configs.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, value);

//    Map<String, Integer> scoreByName = mapper.readValue(jsonSource, Map.class);
//    List<String> names = mapper.readValue(jsonSource, List.class);
//
//    Map<String, ResultValue> results = mapper.readValue(jsonSource,
//        new TypeReference<Map<String, ResultValue>>() {
//        });

    String jsonString = mapper.writeValueAsString(c);

    cw.getWrappedProducer().send(
        new ProducerRecord(CLIENT_CONFIG_COMMANDS_TOPIC, ClientWrapper.APPLICATION_NAME,
            jsonString)).get();
  }

  @NotNull
  private WrapperClientConfg getNewConfig() {
    WrapperClientConfg c = new WrapperClientConfg();
    c.appId = ClientWrapper.APPLICATION_NAME;
    return c;
  }

  @Test
  public void testChangeAcksModeAll()
      throws ExecutionException, InterruptedException, JsonProcessingException {
    WrappedProducer wp = new WrappedProducer(new Properties());
    WrapperClientConfg newConfig = getNewConfig();
    newConfig.configs.put(ProducerConfig.ACKS_CONFIG, "all");
    String jsonString = mapper.writeValueAsString(newConfig);
    wp.send(new ProducerRecord(CLIENT_CONFIG_COMMANDS_TOPIC, ClientWrapper.APPLICATION_NAME,
        jsonString)).get();
  }

  @Test
  public void testChangeAcksModeOne()
      throws ExecutionException, InterruptedException, JsonProcessingException {
    WrappedProducer wp = new WrappedProducer(new Properties());
    WrapperClientConfg newConfig = getNewConfig();
    newConfig.configs.put(ProducerConfig.ACKS_CONFIG, "1");
    String jsonString = mapper.writeValueAsString(newConfig);
    wp.send(new ProducerRecord(CLIENT_CONFIG_COMMANDS_TOPIC, ClientWrapper.APPLICATION_NAME,
        jsonString)).get();
  }

  @Test
  public void testProducerLock() throws ExecutionException, InterruptedException {
    ExecutorService executorService =
        new ThreadPoolExecutor(10, 10, 0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<Runnable>());

    ClientWrapper cw = new ClientWrapper(getBootstrap());

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
    assertThat("thread is blocked on mutex", newProducer.lock.hasQueuedThreads(), is(true)); // should block
    assertThat("thread is blocked on mutex", newProducer.lock.getQueueLength(), greaterThan(0)); // should block

    // try another thread
    // try using the new producer to simulate multi threaded access during the swap
    Runnable anotherRunnable = () -> {
      newProducer.flush(); // should block
    };
    Future<?> submit2 = executorService.submit(anotherRunnable);
    Thread.sleep(PAUSE_TIME); // yield for a second
    assertThat("new client use is blocking", submit2.isDone(), is(false)); // should block
    assertThat("thread is blocked on mutex", newProducer.lock.hasQueuedThreads(), is(true)); // should block
    assertThat("thread is blocked on mutex", newProducer.lock.getQueueLength(), greaterThan(0)); // should block


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
    assertThat("thread is blocked on mutex", newProducer.lock.hasQueuedThreads(), is(false)); // should block
    assertThat("thread is blocked on mutex", newProducer.lock.getQueueLength(), is(0)); // should block

    assertThat("new client use is blocking", submit2.isDone(), is(true)); // should block
    assertThat("new client use is blocking", submit1.isDone(), is(true)); // should block
  }

}
