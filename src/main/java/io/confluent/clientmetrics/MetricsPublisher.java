package io.confluent.clientmetrics;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.clientmetrics.avro.AVROMetricRecords;
import io.confluent.clientmetrics.avro.AVROMetricValue;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;

import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Metrics publisher can be scheduled to run periodically, like:
 * <p>
 * final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
 * executorService.scheduleAtFixedRate(() -> sendMetrics(producer, streamsClient), 0, 1, TimeUnit.SECONDS);
 */
public class MetricsPublisher<V> {

    private static final String DEFAULT_METRICS_TOPIC = "_confluent_client_metrics";

    private final String clientMetricsTopic;
    private final MetricsSender<V> metricsSender;

    public MetricsPublisher(MetricsSender<V> metricsSender) {
        this(metricsSender, DEFAULT_METRICS_TOPIC);
    }

    public MetricsPublisher(MetricsSender<V> metricsSender, String clientMetricsTopic) {
        this.metricsSender = metricsSender;
        this.clientMetricsTopic = clientMetricsTopic;
    }

    public void sendMetrics(KafkaProducer<String, V> kafkaProducer, Supplier<Map<MetricName, ? extends Metric>> metricsSupplier) {
        this.metricsSender.sendMetrics(this.clientMetricsTopic, kafkaProducer, metricsSupplier);
    }

    public interface MetricsSender<V> {
        /**
         * @param kafkaProducer - `key` is not used, String by the contract here
         */
        void sendMetrics(String clientMetricsTopic, KafkaProducer<String, V> kafkaProducer,
                         Supplier<Map<MetricName, ? extends Metric>> metricsSupplier);

        default String extractMetricName(MetricName metricName) {
            return metricName.name() + "_" + metricName.group();
        }

        default String extractMetricValue(Metric metric) {
            return metric.metricValue().toString();
        }

    }

    public static class MetricsAVROSender implements MetricsSender<AVROMetricRecords> {

        public void sendMetrics(String clientMetricsTopic,
                                KafkaProducer<String, AVROMetricRecords> kafkaProducer,
                                Supplier<Map<MetricName, ? extends Metric>> metricsSupplier) {

            final Map<CharSequence, AVROMetricValue> processedMetrics =
                    metricsSupplier.get().entrySet().stream() // TODO Use parallelStream perhaps?
                            .collect(Collectors.toMap(
                                    e -> extractMetricName(e.getKey()), // TODO What about duplicates??? Use tags?
                                    e -> new AVROMetricValue(extractMetricValue(e.getValue())),
                                    (a1, a2) -> a1 // TODO Is there any better way to handle duplicates?
                            ));

            kafkaProducer.send(new ProducerRecord<>(clientMetricsTopic, new AVROMetricRecords(processedMetrics)));
        }
    }

    public static class MetricsJSONSender implements MetricsSender<String> {

        private final ObjectMapper mapper = new ObjectMapper();

        public void sendMetrics(String clientMetricsTopic,
                                KafkaProducer<String, String> kafkaProducer,
                                Supplier<Map<MetricName, ? extends Metric>> metricsSupplier) {

            final Map<String, Object> simpleMetrics =
                    metricsSupplier.get().entrySet().stream() // TODO Use parallelStream perhaps?
                            .collect(Collectors.toMap(
                                    e -> extractMetricName(e.getKey()), // TODO What about duplicates??? Use tags?
                                    // TODO How to present doubles in a better way?
                                    //   ["io-wait-ratio_admin-client-metrics":"2.6899459316018177E-5"]
                                    e -> extractMetricValue(e.getValue()),
                                    (a1, a2) -> a1 // TODO Is there any better way to handle duplicates?
                            ));

            try {
                kafkaProducer.send(new ProducerRecord<>(clientMetricsTopic, mapper.writeValueAsString(simpleMetrics)));
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Failed sending the record to " + clientMetricsTopic, e);
            }
        }
    }
}
