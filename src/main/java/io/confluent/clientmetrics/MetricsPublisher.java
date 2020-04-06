package io.confluent.clientmetrics;

import io.confluent.clientmetrics.avro.MetricNameAVRO;
import io.confluent.clientmetrics.avro.MetricValueAVRO;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractMap.SimpleEntry;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class MetricsPublisher {

    private static final String CLIENT_METRICS_TOPIC = "_confluent_client_metrics";
    private final Logger log = LoggerFactory.getLogger(MetricsPublisher.class);

    public static void main(String[] args) {
        final MetricsPublisher metricsPublisher = new MetricsPublisher();
        metricsPublisher.scheduleBackgroundProcesses();
    }

    public void scheduleBackgroundProcesses() {
        final Admin adminClient = makeKafkaAdminClient();
        final KafkaStreams streamsClient = makeKafkaStreamsClient();
        final KafkaProducer<MetricNameAVRO, MetricValueAVRO> producer = makeKafkaProducer();

        log.info("Load background processes...");
        final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(2);
        executorService.scheduleAtFixedRate(() -> publishKafkaAdminMetrics(producer, adminClient), 0, 1, TimeUnit.SECONDS);
        executorService.scheduleAtFixedRate(() -> publishKafkaStreamsMetrics(producer, streamsClient), 0, 1, TimeUnit.SECONDS);
    }

    private void publishKafkaAdminMetrics(KafkaProducer<MetricNameAVRO, MetricValueAVRO> producer, Admin admin) {
        log.info("Publishing Kafka Admin metrics...");
        sendMetrics(producer, admin.metrics());
    }

    private void publishKafkaStreamsMetrics(KafkaProducer<MetricNameAVRO, MetricValueAVRO> producer, KafkaStreams streams) {
        log.info("Publishing Kafka Streams metrics...");
        sendMetrics(producer, streams.metrics());
    }

    private void sendMetrics(KafkaProducer<MetricNameAVRO, MetricValueAVRO> producer, Map<MetricName, ? extends Metric> metrics) {
        metrics.entrySet().forEach(s -> producer.send(makeRecord(s)));
    }

    private Admin makeKafkaAdminClient() {
        final Properties props = new Properties();
        props.put(AdminClientConfig.CLIENT_ID_CONFIG, "confluent-client-metrics-admin");
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        return AdminClient.create(props);
    }

    private KafkaStreams makeKafkaStreamsClient() {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "confluent-client-metrics-application");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        final StreamsBuilder builder = new StreamsBuilder();
        return new KafkaStreams(builder.build(), props);
    }

    private KafkaProducer<MetricNameAVRO, MetricValueAVRO> makeKafkaProducer() {
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put("schema.registry.url", "http://localhost:8081");

        return new KafkaProducer<>(props);
    }

    private ProducerRecord<MetricNameAVRO, MetricValueAVRO> makeRecord(Map.Entry<MetricName, ? extends Metric> s) {
        return new ProducerRecord<>(CLIENT_METRICS_TOPIC, metricNameAVRO(s), metricValueAVRO(s));
    }

    private MetricNameAVRO metricNameAVRO(Map.Entry<MetricName, ? extends Metric> entry) {
        // TODO Hmm... Might be good to simplify it a bit.
        return new MetricNameAVRO(entry.getKey().name(), entry.getKey().group(),
                entry.getKey().tags().entrySet().stream().map(t -> new SimpleEntry<CharSequence, CharSequence>(
                        t.getKey(), t.getValue())).collect(Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue)));
    }

    private MetricValueAVRO metricValueAVRO(Map.Entry<MetricName, ? extends Metric> entry) {
        return new MetricValueAVRO(entry.getValue().metricValue().toString());
    }
}
