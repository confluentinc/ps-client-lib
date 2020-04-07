package io.confluent.ps.clientwrapper;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public class WrappedConsumer implements Consumer {


  private Consumer wrappedConsumer;


  public static final String KAFKA_SERVER_URL = "localhost";
  public static final int KAFKA_SERVER_PORT = 9092;
  public static final String CLIENT_ID = "SampleConsumer";


  private final static String TOPIC = "my-example-topic";
  private final static String BOOTSTRAP_SERVERS =
      "localhost:9092";

  public Properties customProperties = new Properties();

  public WrappedConsumer(Properties newProperties) {
    this.customProperties = newProperties;

    Properties merged = new Properties();

    // put defaults
    merged.putAll(defaultProperties());

    // override
    merged.putAll(newProperties);

    wrappedConsumer = new KafkaConsumer(merged);
  }

  private Properties defaultProperties() {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "todo");
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringDeserializer");
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringDeserializer");
    return props;
  }

  //////////////////////////////////////////////////
  // Delegate generation
  //////////////////////////////////////////////////

  @Override
  public Set<TopicPartition> assignment() {
    return wrappedConsumer.assignment();
  }

  @Override
  public Set<String> subscription() {
    return wrappedConsumer.subscription();
  }

  @Override
  public void subscribe(Collection topics) {
    wrappedConsumer.subscribe(topics);
  }

  @Override
  public void subscribe(Collection topics,
                        ConsumerRebalanceListener callback) {
    wrappedConsumer.subscribe(topics, callback);
  }

  @Override
  public void assign(Collection collection) {
    wrappedConsumer.assign(collection);
  }

  @Override
  public void subscribe(Pattern pattern,
                        ConsumerRebalanceListener callback) {
    wrappedConsumer.subscribe(pattern, callback);
  }

  @Override
  public void subscribe(Pattern pattern) {
    wrappedConsumer.subscribe(pattern);
  }

  @Override
  public void unsubscribe() {
    wrappedConsumer.unsubscribe();
  }

  @Override
  public ConsumerRecords poll(Duration timeout) {
    return wrappedConsumer.poll(timeout);
  }

  @Override
  public void commitSync() {
    wrappedConsumer.commitSync();
  }

  @Override
  public void commitSync(Duration timeout) {
    wrappedConsumer.commitSync(timeout);
  }

  @Override
  public void commitSync(Map offsets) {
    wrappedConsumer.commitSync(offsets);
  }

  @Override
  public void commitSync(Map offsets, Duration timeout) {
    wrappedConsumer.commitSync(offsets, timeout);
  }

  @Override
  public void commitAsync() {
    wrappedConsumer.commitAsync();
  }

  @Override
  public void commitAsync(OffsetCommitCallback callback) {
    wrappedConsumer.commitAsync(callback);
  }

  @Override
  public void commitAsync(Map offsets,
                          OffsetCommitCallback callback) {
    wrappedConsumer.commitAsync(offsets, callback);
  }

  @Override
  public void seek(TopicPartition partition, long offset) {
    wrappedConsumer.seek(partition, offset);
  }

  @Override
  public void seek(TopicPartition partition,
                   OffsetAndMetadata offsetAndMetadata) {
    wrappedConsumer.seek(partition, offsetAndMetadata);
  }

  @Override
  public void seekToBeginning(Collection collection) {
    wrappedConsumer.seekToBeginning(collection);
  }

  @Override
  public void seekToEnd(Collection collection) {
    wrappedConsumer.seekToEnd(collection);
  }

  @Override
  public long position(TopicPartition partition) {
    return wrappedConsumer.position(partition);
  }

  @Override
  public long position(TopicPartition partition, Duration timeout) {
    return wrappedConsumer.position(partition, timeout);
  }

  @Override
  public OffsetAndMetadata committed(
      TopicPartition partition) {
    return wrappedConsumer.committed(partition);
  }

  @Override
  public OffsetAndMetadata committed(
      TopicPartition partition, Duration timeout) {
    return wrappedConsumer.committed(partition, timeout);
  }

  @Override
  public Map<MetricName, ? extends Metric> metrics() {
    return wrappedConsumer.metrics();
  }

  @Override
  public List<PartitionInfo> partitionsFor(String topic) {
    return wrappedConsumer.partitionsFor(topic);
  }

  @Override
  public List<PartitionInfo> partitionsFor(String topic,
                                           Duration timeout) {
    return wrappedConsumer.partitionsFor(topic, timeout);
  }

  @Override
  public Map<String, List<PartitionInfo>> listTopics() {
    return wrappedConsumer.listTopics();
  }

  @Override
  public Map<String, List<PartitionInfo>> listTopics(
      Duration timeout) {
    return wrappedConsumer.listTopics(timeout);
  }

  @Override
  public Set<TopicPartition> paused() {
    return wrappedConsumer.paused();
  }

  @Override
  public void pause(Collection collection) {
    wrappedConsumer.pause(collection);
  }

  @Override
  public void resume(Collection collection) {
    wrappedConsumer.resume(collection);
  }

  @Override
  public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(
      Map timestampsToSearch) {
    return wrappedConsumer.offsetsForTimes(timestampsToSearch);
  }

  @Override
  public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(
      Map timestampsToSearch, Duration timeout) {
    return wrappedConsumer.offsetsForTimes(timestampsToSearch, timeout);
  }

  @Override
  public Map<TopicPartition, Long> beginningOffsets(
      Collection collection) {
    return wrappedConsumer.beginningOffsets(collection);
  }

  @Override
  public Map<TopicPartition, Long> beginningOffsets(
      Collection collection, Duration timeout) {
    return wrappedConsumer.beginningOffsets(collection, timeout);
  }

  @Override
  public Map<TopicPartition, Long> endOffsets(
      Collection collection) {
    return wrappedConsumer.endOffsets(collection);
  }

  @Override
  public Map<TopicPartition, Long> endOffsets(
      Collection collection, Duration timeout) {
    return wrappedConsumer.endOffsets(collection, timeout);
  }

  @Override
  public void close() {
    wrappedConsumer.close();
  }

  @Override
  public void close(Duration timeout) {
    wrappedConsumer.close(timeout);
  }

  @Override
  public void wakeup() {
    wrappedConsumer.wakeup();
  }

  //////////////////////////////////////////////////
  // Deprecated methods
  //////////////////////////////////////////////////

  @Override
  @Deprecated
  public ConsumerRecords poll(long timeout) {
    throw new DeprecatedException();
  }

  @Override
  @Deprecated
  public void close(long timeout, TimeUnit unit) {
    throw new DeprecatedException();
  }

  @Override
  public Map<TopicPartition, OffsetAndMetadata> committed(Set set) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Map<TopicPartition, OffsetAndMetadata> committed(Set set, Duration duration) {
    throw new UnsupportedOperationException("Not implemented yet");
  }
}
