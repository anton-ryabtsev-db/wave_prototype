package org.wave;

import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.metrics.SourceMetrics;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.Duration;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.wave.WaveLauncher.createPipeline;

@RunWith(JUnit4.class)
public class WaveLauncherTest {
    private static final Logger LOG = LoggerFactory.getLogger(WaveLauncherTest.class);

    public static final String TEST_TOPIC = "kafka-test";

    @Rule
    public TestPipeline p = TestPipeline.create();

    @ClassRule
    public static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:3.2.0"));

    private TestOptions testOptions;
    public KafkaProducer<String, String> producer;

    @Before
    public void before() {
        Map<String, Object> senderProps = new HashMap<>();
        senderProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        senderProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        senderProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producer = new KafkaProducer<>(senderProps);

        testOptions = PipelineOptionsFactory.create().as(TestOptions.class);
        testOptions.setBlockOnRun(false);
        testOptions.setStreaming(true);
        testOptions.setKafkaBootstrapServers(kafka.getBootstrapServers());
        testOptions.setKafkaTopic(TEST_TOPIC);
        testOptions.setKafkaConsumerGroup("wave_beam_app");
        testOptions.setKafkaConsumerOffsetReset("earliest");
    }

    @After
    public void after() {
    }

    @Test
    public void test() {
        PCollection<KV<String, String>> out = createPipeline(p, testOptions);
        PipelineResult result = p.run(testOptions);

        producer.send(new ProducerRecord<>(TEST_TOPIC, "value1"));
        producer.send(new ProducerRecord<>(TEST_TOPIC, "value2"));
        result.waitUntilFinish(Duration.millis(2000));
        MetricName elementsRead = SourceMetrics.elementsRead().getName();
        String readStep = "readFromKafka";
        MetricQueryResults metrics = result
                        .metrics()
                        .queryMetrics(
                                MetricsFilter.builder()
                                        .addNameFilter(MetricNameFilter.named(elementsRead.getNamespace(), elementsRead.getName()))
                                        .build());
        Iterable<MetricResult<Long>> counters = metrics.getCounters();
        // one source
        Long committed = counters.iterator().next().getCommitted();
        assertEquals(2, committed.longValue());
    }

    public interface TestOptions extends DirectOptions, KafkaConsumerOptions {
    }
}
