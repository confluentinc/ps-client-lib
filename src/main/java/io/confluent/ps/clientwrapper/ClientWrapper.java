package io.confluent.ps.clientwrapper;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.xebia.jacksonlombok.JacksonLombokAnnotationIntrospector;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.javers.core.Javers;
import org.javers.core.JaversBuilder;
import org.javers.core.diff.Diff;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientWrapper {

  public static final String CONFLUENT_CLIENT_CONFIG_STORE_NAME = "_confluent_client_config_store";
  Logger log = LoggerFactory.getLogger(ClientWrapper.class);

  public static final String APPLICATION_NAME = "test-app";
  public static String INSTANCE_ID;
  public static final String MY_ID = UUID.randomUUID().toString();
  public static final String CLIENT_META_DATA_TOPIC = "_confluent_client_meta_data";
  public static final String CLIENT_CONFIG_TOPIC = "_confluent_client_config";
  public static final String CLIENT_METRICS_TOPIC = "_confluent_client_metrics";
  public static final int TIMEOUT = 1;

  ObjectMapper mapper = new ObjectMapper()
      .setAnnotationIntrospector(new JacksonLombokAnnotationIntrospector());

  // package private for testing
  WrappedProducer wrappedProducer;
  WrappedConsumer wrappedConsumer;

  KafkaStreams streams;
  private ReadOnlyKeyValueStore<String, String> clientConfigTable;
  private String bootstrapAddress;

  public ClientWrapper(String bootstrap) {
    log.info("Startup...");

    if (StringUtils.isNotEmpty(bootstrap)) {
      this.bootstrapAddress = bootstrap;
    }

    try {
      INSTANCE_ID = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      throw new RuntimeException(e);
    }

    buildKS();

    buildProducer(new Properties());
    buildConsumer(new Properties());

    backgroundProcesses();
  }

  private void buildKS() {
    Properties config = new Properties();
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application");
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
    config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

    // TODO assess what to do here?
//    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.LATEST.toString().toLowerCase());

    StreamsBuilder builder = new StreamsBuilder();
    GlobalKTable<Object, Object> clientConfigTable = builder
        .globalTable(CLIENT_CONFIG_TOPIC, Materialized.as(
            CONFLUENT_CLIENT_CONFIG_STORE_NAME));

    streams = new KafkaStreams(builder.build(), config);
    streams.setUncaughtExceptionHandler((thread, exception) -> {
      log.error(exception.getMessage());
      exception.printStackTrace();
    });
    // Because threads are started in the background, this method does not block. However, if you have global stores in your topology, this method blocks until all global stores are restored.
    streams.start();

    this.clientConfigTable = streams.store(CONFLUENT_CLIENT_CONFIG_STORE_NAME,
        QueryableStoreTypes.<String, String>keyValueStore());

    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

  }

  /**
   * See {@link KafkaProducer#close()}
   */
  // package private for testing
  void buildProducer(Properties props) {
    dumpConfigTable();

    loadConfigBootstrap();

    // Save the old reference
    WrappedProducer oldProducer = this.wrappedProducer;

    // switch the delegate to the new producer first so all new requests go to the new config
    log.trace("Construct and swap to the new client...");
    this.wrappedProducer = new WrappedProducer(props);

    // if the old producer existed (it will unless it's the first construction), close it. Outstanding requests get completed first, this will block.
    if (oldProducer != null) {
      log.info("Close the old client...");
      oldProducer.close();
    }

    registerMe();

    getWrappedProducer().unblock();
  }

  private Properties loadConfigBootstrap() {
    String aLong = this.clientConfigTable.get(this.getMyId());
    Properties p = new Properties();
    if (aLong != null) {
      p.put("thing", aLong);
    }
    return p;
  }

  private void dumpConfigTable() {
    KeyValueIterator<String, String> all = clientConfigTable.all();
    StringBuffer sb = new StringBuffer("Dump config GKT content:\n");
    while (all.hasNext()) {
      KeyValue<String, String> next = all.next();
      sb.append(StringUtils.rightPad(next.key, 20) + " : " + next.value);
    }
    log.trace(sb.toString());
  }

  /**
   * @see KafkaConsumer#close()
   */
  private void buildConsumer(Properties props) {
    // Save the old reference
    WrappedConsumer oldConsumer = this.wrappedConsumer;

    // switch the delegate to the new producer first so all new requests go to the new config
    log.trace("Construct and swap to the new client...");
    this.wrappedConsumer = new WrappedConsumer(props);

    // subscribe to config topic
    getWrappedConsumer().subscribe(Arrays.asList(CLIENT_CONFIG_TOPIC));

    // if the old producer existed (it will unless it's the first construction), close it. Outstanding requests get completed first, this will block.
    if (oldConsumer != null) {
      log.info("Close the old client...");
      oldConsumer.close();
    }
  }

  private void backgroundProcesses() {
    log.info("Load background processes...");
    Runnable config = configWatcherProcess();
    Runnable metrics = metricsPublisher();
    ExecutorService executorService =
        new ThreadPoolExecutor(2, 2, 0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<Runnable>());
    executorService.execute(config);
    executorService.execute(metrics);
  }

  private Runnable metricsPublisher() {
    log.info("Start metrics publisher...");
    Runnable metricsPublisher = () -> {
      // change to non blocking wait
      try {
        while (true) {
          log.info("Publish metrics...");
          publishMetrics();
          long sleepTimeMillis = (long) (10000 * Math.random());
          TimeUnit.MILLISECONDS.sleep(sleepTimeMillis);
          //Thread.sleep((long) Math.random() * Duration.ofMinutes(2).toMillis());
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    };
    return metricsPublisher;
  }

  private void publishMetrics() {
    mySend(CLIENT_METRICS_TOPIC, "Some metrics...");
  }

  private Runnable configWatcherProcess() {
    log.info("Start config watcher...");
    // start monitoring for config changes
    // TODO Switch to processor supervisor
    Runnable configWatcher = () -> {
      while (true) {
        log.debug("Poll for configs...");
        ConsumerRecords records = getWrappedConsumer().poll(Duration.ofSeconds(5));
        records.forEach(cr -> {
          processConfigMessage((ConsumerRecord) cr);
        });
      }
    };
    return configWatcher;
  }

  private void processConfigMessage(ConsumerRecord cr) {
    log.debug("Config message received: " + cr.value());
    reconfigureClients((String) cr.value());
  }

  private void reconfigureClients(String value) {
    // TODO switch either producer or consumer configs or both?
    // TODO actually extract properties
    // Parse the new config
    WrapperClientConfg c = null;
    try {
      c = mapper.readValue(value, WrapperClientConfg.class);
    } catch (IOException e) {
      log.error("Error parsing config message, failing.", e);
      return;
    }
    log.info("Received new config: " + c.toString());

    Map configMap = c.configs;
    Properties newProducerProps = new Properties();
    newProducerProps.putAll(configMap);

    // merge with bootstrap configs - GTK is only eventually update with this new config set and may or may not have been updated already
    Properties bootstrap = loadConfigBootstrap();

    // lets compare them for fun
    Javers javers = JaversBuilder.javers().build();
    Diff compare = javers.compare(bootstrap, newProducerProps);
    log.info(
        "What's difference in the signaled config vs the bootstrap table _now_:\n" + compare
            .prettyPrint());

    // merge them
    newProducerProps.putAll(bootstrap);

    newProducerProps.put(ProducerConfig.CLIENT_ID_CONFIG,
        "KafkaExampleProducer-" + UUID.randomUUID().toString());
    log.info("Replace the old producer...");
    buildProducer(newProducerProps);

    // Parse the new config
    Properties newConsumerProps = new Properties();
    // TODO remove random client id and check JMX collision doesn't happen
    newConsumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG,
        "KafkaExampleConsumer-" + UUID.randomUUID().toString());
    log.info("Replace the old consumer...");
    buildConsumer(newConsumerProps);
  }

  private void registerMe() {
    log.info("Send init info...");
    String version = getVersion();
    mySend(CLIENT_META_DATA_TOPIC, getVersion());
  }

  private void mySend(String topicName, Object value) {
    try {
      String defaultKey = APPLICATION_NAME + "-" + INSTANCE_ID;
      getWrappedProducer().send(new ProducerRecord(topicName, defaultKey, value))
          .get(TIMEOUT, TimeUnit.MINUTES);
    } catch (Exception e) {
      throw new ClientWrapperRuntimeException(e);
    }
  }

  private String getMyId() {
    return MY_ID;
  }

  private String getVersion() {
    // TODO make dynamic
    // TODO publish producer and consumer metrics?
    String ver = AppInfoParser.getVersion();
    String sha = AppInfoParser.getCommitId();
    Properties customProperties = getWrappedProducer().customProperties;
    Object clientId = customProperties.get(ProducerConfig.CLIENT_ID_CONFIG);
    return ver + "-" + clientId + "-" + sha;
  }

  public WrappedProducer getWrappedProducer() {
    return wrappedProducer;
  }

  public WrappedConsumer getWrappedConsumer() {
    return wrappedConsumer;
  }
}
