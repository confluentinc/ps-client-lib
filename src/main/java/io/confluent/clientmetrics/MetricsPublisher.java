package io.confluent.clientmetrics;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.clientmetrics.avro.MetricNameAVRO;
import io.confluent.clientmetrics.avro.MetricValueAVRO;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;

import java.util.AbstractMap.SimpleEntry;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Metrics publisher can be scheduled to run periodically, like:
 * <p>
 * final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
 * executorService.scheduleAtFixedRate(() -> sendMetrics(producer, streamsClient), 0, 1, TimeUnit.SECONDS);
 */
public class MetricsPublisher<K, V> {

  private static final String DEFAULT_METRICS_TOPIC = "_confluent_client_metrics";

  private final String clientMetricsTopic;
  private final MetricsPublisherSender<K, V> actualPublisher;

  public MetricsPublisher(MetricsPublisherSender<K, V> actualPublisher) {
    this(actualPublisher, DEFAULT_METRICS_TOPIC);
  }

  public MetricsPublisher(MetricsPublisherSender<K, V> actualPublisher, String clientMetricsTopic) {
    this.actualPublisher = actualPublisher;
    this.clientMetricsTopic = clientMetricsTopic;
  }

  public void sendMetrics(KafkaProducer<K, V> kafkaProducer, Supplier<Map<MetricName, ? extends Metric>> metricsSupplier) {
    this.actualPublisher.sendMetrics(this.clientMetricsTopic, kafkaProducer, metricsSupplier);
  }

  public interface MetricsPublisherSender<K, V> {
    void sendMetrics(String clientMetricsTopic, KafkaProducer<K, V> kafkaProducer,
                     Supplier<Map<MetricName, ? extends Metric>> metricsSupplier);
  }

  public static class MetricsPublisherAVROSender implements MetricsPublisherSender<MetricNameAVRO, MetricValueAVRO> {

    public void sendMetrics(String clientMetricsTopic,
                            KafkaProducer<MetricNameAVRO, MetricValueAVRO> kafkaProducer,
                            Supplier<Map<MetricName, ? extends Metric>> metricsSupplier) {
      metricsSupplier.get()
              .forEach((key, value) -> kafkaProducer.send(makeRecord(clientMetricsTopic, key, value)));
    }

    private ProducerRecord<MetricNameAVRO, MetricValueAVRO> makeRecord(String clientMetricsTopic,
                                                                       MetricName name, Metric metric) {
      return new ProducerRecord<>(clientMetricsTopic, metricNameAVRO(name), metricValueAVRO(metric));
    }

    private MetricNameAVRO metricNameAVRO(MetricName name) {
      return new MetricNameAVRO(name.name(), name.group(),
          name.tags().entrySet().stream().map(t -> new SimpleEntry<CharSequence, CharSequence>(t.getKey(), t.getValue()))
              .collect(Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue)));
    }

    private MetricValueAVRO metricValueAVRO(Metric metric) {
      return new MetricValueAVRO(metric.metricValue().toString());
    }
  }

  public static class MetricsPublisherJSONSender implements MetricsPublisherSender<String, String> {

    private final ObjectMapper mapper = new ObjectMapper();

    public void sendMetrics(String clientMetricsTopic,
                            KafkaProducer<String, String> kafkaProducer,
                            Supplier<Map<MetricName, ? extends Metric>> metricsSupplier) {

      final Map<String, Object> simpleMetrics =
          metricsSupplier.get().entrySet().stream() // TODO Use parallelStream perhaps?
              .collect(Collectors.toMap(
                  e -> e.getKey().name() + "_" + e.getKey().group(), // TODO What about duplicates???
                  // TODO How to present doubles in a better way?
                  //   ["io-wait-ratio_admin-client-metrics":2.6899459316018177E-5]
                  e -> e.getValue().metricValue(),
                  // TODO Is there any better way to handle duplicates?
                  (a1, a2) -> a1
              ));

      try {
        kafkaProducer.send(new ProducerRecord<>(clientMetricsTopic, mapper.writeValueAsString(simpleMetrics)));
      } catch (JsonProcessingException e) {
        throw new RuntimeException("Failed sending the record to " + clientMetricsTopic, e);
      }
    }
  }
}
