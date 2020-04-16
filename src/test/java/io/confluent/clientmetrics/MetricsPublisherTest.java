package io.confluent.clientmetrics;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.confluent.clientmetrics.MetricsPublisher.MetricsPublisherAVROSender;
import io.confluent.clientmetrics.MetricsPublisher.MetricsPublisherJSONSender;
import io.confluent.clientmetrics.MetricsPublisher.MetricsPublisherSender;
import io.confluent.clientmetrics.avro.MetricNameAVRO;
import io.confluent.clientmetrics.avro.MetricValueAVRO;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.assertj.core.groups.Tuple;
import org.junit.Rule;
import org.junit.Test;
import org.rnorth.ducttape.unreliables.Unreliables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

public class MetricsPublisherTest {

  private final Logger log = LoggerFactory.getLogger(MetricsPublisherTest.class);
  private final ObjectMapper mapper = new ObjectMapper();

  @Rule
  public KafkaContainer kafka = new KafkaContainer();

  @Test
  public void publishMetricsForAdminClientAsString() {
    final String topicName = "_metrics_" + UUID.randomUUID().toString();
    final MetricsPublisherSender<String, String> protocol = new MetricsPublisherJSONSender();
    final MetricsPublisher<String, String> metricsPublisher = new MetricsPublisher<>(protocol, topicName);
    final Admin adminClient = makeKafkaAdminClient();

    final KafkaProducer<String, String> producer = new KafkaProducer<>(
        ImmutableMap.of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
            ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString()
        ),
        new StringSerializer(), new StringSerializer()
    );

    final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(
        ImmutableMap.of(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
            ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString(),
            ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString(),
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
        ),
        new StringDeserializer(), new StringDeserializer()
    );

    try (producer; consumer) {
      metricsPublisher.sendMetrics(producer, adminClient::metrics);

      consumer.subscribe(Collections.singletonList(topicName));

      Unreliables.retryUntilTrue(20, TimeUnit.SECONDS, () -> {
        final ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

        if (records.isEmpty()) {
          return false;
        }

        records.forEach(s -> log.info(s.value()));

        assertThat(records).hasSize(1)
            .extracting(ConsumerRecord::topic, ConsumerRecord::serializedKeySize)
            .containsExactly(Tuple.tuple(topicName, -1));

        assertThat(records)
            .flatExtracting(s -> mapper.readValue(s.value(), Map.class).keySet())
            .contains("version_app-info", "request-total_admin-client-metrics");

        return true;
      });

      consumer.unsubscribe();
    }
  }

  @Test
  public void publishMetricsForStreamsClientAsString() {
    final String topicName = "_metrics_" + UUID.randomUUID().toString();
    final MetricsPublisherSender<String, String> protocol = new MetricsPublisherJSONSender();
    final MetricsPublisher<String, String> metricsPublisher = new MetricsPublisher<>(protocol, topicName);
    final KafkaStreams streamsClient = makeKafkaStreamsClient();

    final KafkaProducer<String, String> producer = new KafkaProducer<>(
        ImmutableMap.of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
            ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString()
        ),
        new StringSerializer(), new StringSerializer()
    );

    final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(
        ImmutableMap.of(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
            ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString(),
            ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString(),
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
        ),
        new StringDeserializer(), new StringDeserializer()
    );

    try (producer; consumer) {
      metricsPublisher.sendMetrics(producer, streamsClient::metrics);

      consumer.subscribe(Collections.singletonList(topicName));

      Unreliables.retryUntilTrue(20, TimeUnit.SECONDS, () -> {
        final ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

        if (records.isEmpty()) {
          return false;
        }

        records.forEach(s -> log.info(s.value()));

        assertThat(records).hasSize(1)
            .extracting(ConsumerRecord::topic, ConsumerRecord::serializedKeySize)
            .containsExactly(Tuple.tuple(topicName, -1));

        assertThat(records)
            .flatExtracting(s -> mapper.readValue(s.value(), Map.class).keySet())
            .contains("version_app-info", "request-total_admin-client-metrics", "skipped-records-rate_stream-metrics");

        return true;
      });

      consumer.unsubscribe();
    }
  }

  @Test
  public void publishMetricsForAdminClientAsAVRO() {
    final Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
    props.put("schema.registry.url", "http://localhost:8081");

    final MetricsPublisherSender<MetricNameAVRO, MetricValueAVRO> protocol = new MetricsPublisherAVROSender();
    final MetricsPublisher<MetricNameAVRO, MetricValueAVRO> metricsPublisher = new MetricsPublisher<>(protocol);
    final KafkaProducer<MetricNameAVRO, MetricValueAVRO> producer = new KafkaProducer<>(props);
    final Admin adminClient = makeKafkaAdminClient();

    log.info("Publishing Kafka Admin metrics...");
    metricsPublisher.sendMetrics(producer, adminClient::metrics);
  }

  @Test
  public void publishMetricsForStreamsClientAsAVRO() {
    final Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
    props.put("schema.registry.url", "http://localhost:8081");

    final MetricsPublisherSender<MetricNameAVRO, MetricValueAVRO> protocol = new MetricsPublisherAVROSender();
    final MetricsPublisher<MetricNameAVRO, MetricValueAVRO> metricsPublisher = new MetricsPublisher<>(protocol);
    final KafkaProducer<MetricNameAVRO, MetricValueAVRO> producer = new KafkaProducer<>(props);
    final KafkaStreams streamsClient = makeKafkaStreamsClient();

    log.info("Publishing Kafka Streams metrics...");
    metricsPublisher.sendMetrics(producer, streamsClient::metrics);
  }

  private Admin makeKafkaAdminClient() {
    final Properties props = new Properties();
    props.put(AdminClientConfig.CLIENT_ID_CONFIG, "test-client-metrics-admin");
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());

    return AdminClient.create(props);
  }

  private KafkaStreams makeKafkaStreamsClient() {
    final Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-client-metrics-application");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

    final StreamsBuilder builder = new StreamsBuilder();
    return new KafkaStreams(builder.build(), props);
  }
}