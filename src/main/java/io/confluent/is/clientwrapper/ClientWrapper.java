package io.confluent.is.clientwrapper;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.internals.Sender;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.utils.LogContext;

public class ClientWrapper implements Producer {

  private static final String META_DATA_TOPIC_NAME = "_confluent_client_meta_data";
  public static final int TIMEOUT = 1;
  private final KafkaProducer kafkaProducer = new KafkaProducer();
  Producer myProducer = new KafkaProducer(new Properties());
  Consumer configConsumer = new KafkaConsumer(new Properties());
  public static final String MY_ID = UUID.randomUUID().toString();

  ClientWrapper() throws InterruptedException, ExecutionException, TimeoutException {
    registerMe();
  }

  private void registerMe() {
    String version = getVersion();
    mySend(getVersion());
  }

  private void mySend(Object value) {
    try {
      myProducer.send(new ProducerRecord(META_DATA_TOPIC_NAME, value))
          .get(TIMEOUT, TimeUnit.MINUTES);
    } catch (Exception e) {
      throw new ClientWrapperException(e);
    }
  }

  private String getMyId() {
    return MY_ID;
  }

  private String getVersion() {
    // TODO make dynamic
    return "2.1.1";
  }

  public KafkaProducer getKafkaProducer() {
    return kafkaProducer;
  }

  Sender newSender(
      LogContext logContext,
      KafkaClient kafkaClient, Metadata metadata) {
    return kafkaProducer.newSender(logContext, kafkaClient, metadata);
  }

  public void initTransactions() {
    kafkaProducer.initTransactions();
  }

  public void beginTransaction() throws ProducerFencedException {
    kafkaProducer.beginTransaction();
  }

  public void sendOffsetsToTransaction(Map offsets, String consumerGroupId)
      throws ProducerFencedException {
    kafkaProducer.sendOffsetsToTransaction(offsets, consumerGroupId);
  }

  public void commitTransaction() throws ProducerFencedException {
    kafkaProducer.commitTransaction();
  }

  public void abortTransaction() throws ProducerFencedException {
    kafkaProducer.abortTransaction();
  }

  public Future<RecordMetadata> send(
      ProducerRecord record) {
    return kafkaProducer.send(record);
  }

  public Future<RecordMetadata> send(
      ProducerRecord record,
      Callback callback) {
    return kafkaProducer.send(record, callback);
  }

  public void flush() {
    kafkaProducer.flush();
  }

  public List<PartitionInfo> partitionsFor(String topic) {
    return kafkaProducer.partitionsFor(topic);
  }

  public Map<MetricName, ? extends Metric> metrics() {
    return kafkaProducer.metrics();
  }

  public void close() {
    kafkaProducer.close();
  }

  public void close(Duration timeout) {
    kafkaProducer.close(timeout);
  }

  String getClientId() {
    return kafkaProducer.getClientId();
  }
}
