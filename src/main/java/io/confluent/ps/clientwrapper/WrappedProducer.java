package io.confluent.ps.clientwrapper;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.internals.Sender;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.LogContext;

public class WrappedProducer implements Producer {

  private Producer delegateProducer;

  public static final String KAFKA_SERVER_URL = "localhost";
  public static final int KAFKA_SERVER_PORT = 9092;
  public static final String CLIENT_ID = "SampleConsumer";


  private final static String TOPIC = "my-example-topic";
  private final static String BOOTSTRAP_SERVERS =
      "localhost:9092";

  public Properties customProperties = new Properties();

  public WrappedProducer(Properties newProperties) {
    this.customProperties = newProperties;

    Properties merged = new Properties();
    // first put defaults
    merged.putAll(defaultProperties());

    // override
    merged.putAll(newProperties);

    delegateProducer = new KafkaProducer(merged);
  }

  private Properties defaultProperties() {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    return props;
  }

  //////////////////////////////////////////////////
  // Delegate generation
  //////////////////////////////////////////////////


  public Producer getDelegateProducer() {
    return delegateProducer;
  }

  public void initTransactions() {
    delegateProducer.initTransactions();
  }

  public void beginTransaction() throws ProducerFencedException {
    delegateProducer.beginTransaction();
  }

  public void sendOffsetsToTransaction(Map offsets, String consumerGroupId)
      throws ProducerFencedException {
    delegateProducer.sendOffsetsToTransaction(offsets, consumerGroupId);
  }

  public void commitTransaction() throws ProducerFencedException {
    delegateProducer.commitTransaction();
  }

  public void abortTransaction() throws ProducerFencedException {
    delegateProducer.abortTransaction();
  }

  public Future<RecordMetadata> send(
      ProducerRecord record) {
    return delegateProducer.send(record);
  }

  public Future<RecordMetadata> send(
      ProducerRecord record,
      Callback callback) {
    return delegateProducer.send(record, callback);
  }

  public void flush() {
    delegateProducer.flush();
  }

  public List<PartitionInfo> partitionsFor(String topic) {
    return delegateProducer.partitionsFor(topic);
  }

  public Map<MetricName, ? extends Metric> metrics() {
    return delegateProducer.metrics();
  }

  public void close() {
    delegateProducer.close();
  }

  public void close(Duration timeout) {
    delegateProducer.close(timeout);
  }


}
