package org.wave;

import com.google.common.collect.ImmutableMap;
import com.ibm.mq.MQException;
import com.ibm.mq.jms.MQQueue;
import com.ibm.mq.jms.MQQueueConnectionFactory;
import org.apache.beam.runners.dataflow.TestDataflowPipelineOptions;
import org.apache.beam.runners.dataflow.TestDataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.metrics.SinkMetrics;
import org.apache.beam.sdk.metrics.SourceMetrics;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
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
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.ibm.msg.client.jms.JmsConstants.AUTO_ACKNOWLEDGE;
import static com.ibm.msg.client.jms.JmsConstants.CONNECTION_TYPE;
import static org.apache.beam.runners.dataflow.options.DataflowWorkerLoggingOptions.Level.DEBUG;
import static org.apache.beam.sdk.testing.TestPipeline.PROPERTY_BEAM_TEST_PIPELINE_OPTIONS;
import static org.apache.beam.sdk.testing.TestPipeline.testingPipelineOptions;
import static org.junit.Assert.assertEquals;
import static org.wave.WaveLauncher.createPipeline;
import static org.wave.WaveLauncher.getMqQueueConnectionFactory;

@RunWith(JUnit4.class)
public class WaveLauncherDFTest {
    private static final Logger LOG = LoggerFactory.getLogger(WaveLauncherTest.class);

    public static final String TEST_TOPIC = "kafka-test";
    static {
        System.setProperty(PROPERTY_BEAM_TEST_PIPELINE_OPTIONS , "[\"--runner=TestDataflowRunner\",\"--project=sandbox-307310\",\"--tempRoot=gs://pub_sub_example\", \"--jobName=kafka-test\", \"--region=europe-west3\"]");

        //Set in test configuration
        System.setProperty("GOOGLE_APPLICATION_CREDENTIALS", "D:\\git_repos\\sandbox-307310-1b2ed2f2ab37.json");
    }

    @Rule
    public TestPipeline p = TestPipeline.create();

    private TestOptions testOptions;
    private KafkaProducer<String, String> kafkaProducer;
    private MessageProducer producer;
    private Session session;

    @Before
    public void before() throws JMSException {
        Map<String, Object> senderProps = new HashMap<>();
        senderProps.put("bootstrap.servers", "34.106.184.23:9092");
        senderProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        senderProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
//        kafkaProducer = new KafkaProducer<>(senderProps);

        MQQueueConnectionFactory mqQueueConnectionFactory = getMqQueueConnectionFactory("34.89.247.3", 1414);
        session = mqQueueConnectionFactory.createConnection().createSession(false, AUTO_ACKNOWLEDGE);
        MQQueue mqQueue = new MQQueue("DEV.QUEUE.1");
        producer = session.createProducer(mqQueue);

        testOptions = testingPipelineOptions().as(TestOptions.class);
        testOptions.setRunner(TestDataflowRunner.class);
//        testOptions.setDefaultWorkerLogLevel(DEBUG);
        testOptions.setStreaming(true);
        testOptions.setKafkaBootstrapServers("34.106.184.23:9092");
        testOptions.setKafkaTopic(TEST_TOPIC);
        testOptions.setKafkaConsumerGroup("wave_beam_app");
        testOptions.setKafkaConsumerOffsetReset("earliest");
    }

    @After
    public void after() {
    }

    @Test
    public void test() throws JMSException {
//        producer.send(session.createTextMessage("jms1"));
//        producer.send(session.createTextMessage("jms2"));
//        producer.send(session.createTextMessage("jms3"));
//        producer.send(session.createTextMessage("jms4"));
//        producer.send(session.createTextMessage("jms5"));
//        producer.send(session.createTextMessage("jms6"));
//        producer.send(session.createTextMessage("jms7"));
//        producer.send(session.createTextMessage("jms8"));

        PCollection<String> output = createPipeline(p, testOptions, "34.89.247.3", 1414);
//        PAssert.that(output).containsInAnyOrder("jms1", "kafka1", "kafka2");
        PipelineResult result = p.run(testOptions);

//        Future<RecordMetadata> kafka1 = kafkaProducer.send(new ProducerRecord<>(TEST_TOPIC, "kafka1"));
//        try {
//            RecordMetadata recordMetadata = kafka1.get();
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        } catch (ExecutionException e) {
//            e.printStackTrace();
//        }
//
//        kafkaProducer.send(new ProducerRecord<>(TEST_TOPIC, "kafka2"));
//        kafkaProducer.send(new ProducerRecord<>(TEST_TOPIC, "kafka3"));
//        kafkaProducer.send(new ProducerRecord<>(TEST_TOPIC, "kafka4"));
//        kafkaProducer.send(new ProducerRecord<>(TEST_TOPIC, "kafka5"));
        result.waitUntilFinish(Duration.millis(2000));
        MetricName elementsRead = SourceMetrics.elementsRead().getName();
        MetricName elementsWritten = SinkMetrics.elementsWritten().getName();
        MetricResults allMetrics = result
                .metrics();
        MetricQueryResults sourceMetrics = allMetrics.queryMetrics(
                MetricsFilter.builder()
                        .addNameFilter(MetricNameFilter.named(elementsRead.getNamespace(), elementsRead.getName()))
                        .build());
        MetricQueryResults sinkMetrics = allMetrics.queryMetrics(
                MetricsFilter.builder()
                        .addNameFilter(MetricNameFilter.named(elementsWritten.getNamespace(), elementsWritten.getName()))
                        .build());
        Iterable<MetricResult<Long>> sourceCounters = sourceMetrics.getCounters();
        Iterable<MetricResult<Long>> sinkCounters = sinkMetrics.getCounters();
        // kafka source
        Long sourceCommitted = sourceCounters.iterator().next().getCommitted();
        assertEquals(2, sourceCommitted.longValue());
        System.out.println(result.getState());
//        Long sinkCommitted = sinkCounters.iterator().next().getCommitted();
//        assertEquals(3, sinkCommitted.longValue());

    }

    public interface TestOptions extends TestDataflowPipelineOptions, KafkaConsumerOptions {
    }
}
