package io.confluent.clientmetrics;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.confluent.clientmetrics.MetricsPublisher.MetricsAVROSender;
import io.confluent.clientmetrics.MetricsPublisher.MetricsJSONSender;
import io.confluent.clientmetrics.MetricsPublisher.MetricsSender;
import io.confluent.clientmetrics.avro.AVROMetricRecords;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
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
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class MetricsPublisherTest {

    private final Logger log = LoggerFactory.getLogger(MetricsPublisherTest.class);
    private final ObjectMapper mapper = new ObjectMapper();

    @Rule
    public KafkaContainer kafka = new KafkaContainer("5.4.1");

    @Test
    public void publishMetricsForAdminClientAsJSON() {
        final String topicName = "_metrics_" + UUID.randomUUID().toString();
        final MetricsSender<String> metricsSender = new MetricsJSONSender();
        final MetricsPublisher<String> metricsPublisher = new MetricsPublisher<>(metricsSender, topicName);
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

                assertThat(records).hasSize(1)
                        .extracting(ConsumerRecord::topic, ConsumerRecord::serializedKeySize)
                        .containsExactly(Tuple.tuple(topicName, -1));

                printJSON(records);

                assertThat(records)
                        .flatExtracting(s -> mapper.readValue(s.value(), new TypeReference<Map<String, String>>() {
                        }).keySet())
                        .contains("version_app-info", "request-total_admin-client-metrics", "count_kafka-metrics-count");

                return true;
            });

            consumer.unsubscribe();
        }
    }

    @Test
    public void publishMetricsForStreamsClientAsJSON() {
        final String topicName = "_metrics_" + UUID.randomUUID().toString();
        final MetricsSender<String> metricsSender = new MetricsJSONSender();
        final MetricsPublisher<String> metricsPublisher = new MetricsPublisher<>(metricsSender, topicName);
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

                assertThat(records).hasSize(1)
                        .extracting(ConsumerRecord::topic, ConsumerRecord::serializedKeySize)
                        .containsExactly(Tuple.tuple(topicName, -1));

                printJSON(records);

                assertThat(records)
                        .flatExtracting(s -> mapper.readValue(s.value(), new TypeReference<Map<String, String>>() {
                        }).keySet())
                        .contains("version_app-info", "poll-rate_stream-metrics", "application-id_stream-metrics");

                return true;
            });

            consumer.unsubscribe();
        }
    }

    private void printJSON(ConsumerRecords<String, String> records) {
        records.forEach(rec -> {
            try {
                mapper.readValue(rec.value(), new TypeReference<Map<String, String>>() {
                }).forEach((key, value) -> log.info(key + " = " + value));
            } catch (JsonProcessingException ignored) {
            }
        });
    }

    private void printAVRO(ConsumerRecords<String, AVROMetricRecords> records) {
        records.forEach(rec -> rec.value().getMetrics().forEach((key, value) -> log.info(key + " = " + value)));
    }

    @Test
    public void publishMetricsForAdminClientAsAVRO() {
        final String topicName = "_metrics_" + UUID.randomUUID().toString();
        final MetricsSender<AVROMetricRecords> metricsSender = new MetricsAVROSender();
        final MetricsPublisher<AVROMetricRecords> metricsPublisher = new MetricsPublisher<>(metricsSender, topicName);
        final Admin adminClient = makeKafkaAdminClient();

        final SchemaRegistryContainer schemaRegistry = new SchemaRegistryContainer("5.4.1");

        schemaRegistry.withKafka(kafka)
                .withLogConsumer(new Slf4jLogConsumer(log))
                .waitingFor(Wait.forHttp("/subjects").forStatusCode(200))
                .start();

        final KafkaProducer<String, AVROMetricRecords> producer = new KafkaProducer<>(
                Map.of(
                        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName(),
                        KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistry.getTarget(),
                        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                        ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString()
                ));

        final KafkaConsumer<String, AVROMetricRecords> consumer = new KafkaConsumer<>(
                Map.of(
                        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName(),
                        KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistry.getTarget(),
                        KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true,
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                        ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString(),
                        ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString(),
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
                ));

        try (producer; consumer; schemaRegistry) {
            metricsPublisher.sendMetrics(producer, adminClient::metrics);

            consumer.subscribe(Collections.singletonList(topicName));

            Unreliables.retryUntilTrue(20, TimeUnit.SECONDS, () -> {
                final ConsumerRecords<String, AVROMetricRecords> records = consumer.poll(Duration.ofMillis(100));

                if (records.isEmpty()) {
                    return false;
                }

                assertThat(records).hasSize(1)
                        .extracting(ConsumerRecord::topic, ConsumerRecord::serializedKeySize)
                        .containsExactly(Tuple.tuple(topicName, -1));

                printAVRO(records);

                assertThat(records)
                        .flatExtracting(s -> s.value().getMetrics().keySet().stream().map(Object::toString).collect(Collectors.toSet()))
                        .contains("version_app-info", "request-total_admin-client-metrics", "count_kafka-metrics-count");

                return true;
            });

            consumer.unsubscribe();
        }
    }

    @Test
    public void publishMetricsForStreamsClientAsAVRO() {
        final String topicName = "_metrics_" + UUID.randomUUID().toString();
        final MetricsSender<AVROMetricRecords> metricsSender = new MetricsAVROSender();
        final MetricsPublisher<AVROMetricRecords> metricsPublisher = new MetricsPublisher<>(metricsSender, topicName);
        final KafkaStreams streamsClient = makeKafkaStreamsClient();

        final SchemaRegistryContainer schemaRegistry = new SchemaRegistryContainer("5.4.1");

        schemaRegistry.withKafka(kafka)
                .withLogConsumer(new Slf4jLogConsumer(log))
                .waitingFor(Wait.forHttp("/subjects").forStatusCode(200))
                .start();

        final KafkaProducer<String, AVROMetricRecords> producer = new KafkaProducer<>(
                Map.of(
                        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName(),
                        KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistry.getTarget(),
                        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                        ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString()
                ));

        final KafkaConsumer<String, AVROMetricRecords> consumer = new KafkaConsumer<>(
                Map.of(
                        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName(),
                        KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistry.getTarget(),
                        KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true,
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                        ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString(),
                        ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString(),
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
                ));

        try (producer; consumer; schemaRegistry) {
            metricsPublisher.sendMetrics(producer, streamsClient::metrics);

            consumer.subscribe(Collections.singletonList(topicName));

            Unreliables.retryUntilTrue(20, TimeUnit.SECONDS, () -> {
                final ConsumerRecords<String, AVROMetricRecords> records = consumer.poll(Duration.ofMillis(100));

                if (records.isEmpty()) {
                    return false;
                }

                assertThat(records).hasSize(1)
                        .extracting(ConsumerRecord::topic, ConsumerRecord::serializedKeySize)
                        .containsExactly(Tuple.tuple(topicName, -1));

                printAVRO(records);

                assertThat(records)
                        .flatExtracting(s -> s.value().getMetrics().keySet().stream().map(Object::toString).collect(Collectors.toSet()))
                        .contains("version_app-info", "poll-rate_stream-metrics", "application-id_stream-metrics");

                return true;
            });

            consumer.unsubscribe();
        }
    }

    private Admin makeKafkaAdminClient() {
        final Properties props = new Properties();
        props.put(AdminClientConfig.CLIENT_ID_CONFIG, "test-client-metrics-admin-" + UUID.randomUUID());
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());

        return AdminClient.create(props);
    }

    private KafkaStreams makeKafkaStreamsClient() {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-client-metrics-application-" + UUID.randomUUID());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        final StreamsBuilder builder = new StreamsBuilder();
        return new KafkaStreams(builder.build(), props);
    }
}