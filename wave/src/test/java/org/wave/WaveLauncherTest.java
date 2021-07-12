package org.wave;

import com.google.common.collect.ImmutableMap;
import com.ibm.mq.jms.MQQueue;
import com.ibm.mq.jms.MQQueueConnectionFactory;
import org.apache.beam.runners.dataflow.TestDataflowRunner;
import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.metrics.SourceMetrics;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
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
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import java.util.HashMap;
import java.util.Map;

import static com.ibm.msg.client.jms.JmsConstants.AUTO_ACKNOWLEDGE;
import static org.apache.beam.runners.dataflow.options.DataflowWorkerLoggingOptions.Level.DEBUG;
import static org.apache.beam.sdk.testing.TestPipeline.PROPERTY_BEAM_TEST_PIPELINE_OPTIONS;
import static org.apache.beam.sdk.testing.TestPipeline.testingPipelineOptions;
import static org.junit.Assert.assertEquals;
import static org.wave.WaveLauncher.createPipeline;
import static org.wave.WaveLauncher.getMqQueueConnectionFactory;

@RunWith(JUnit4.class)
public class WaveLauncherTest {
    private static final Logger LOG = LoggerFactory.getLogger(WaveLauncherTest.class);

    public static final String TEST_TOPIC = "kafka-test";

    @Rule
    public TestPipeline p = TestPipeline.create();

    @ClassRule
    public static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:3.2.0"));
    @ClassRule
    public static GenericContainer mq = new GenericContainer(DockerImageName.parse("ibmcom/mq:9.2.2.0-r1"))
            .withEnv(ImmutableMap.of("MQ_QMGR_NAME", "mq_queue",
                    "LICENSE", "accept"))
            .withExposedPorts(1414, 9443);

    private TestOptions testOptions;
    private KafkaProducer<String, String> kafkaProducer;
    private MessageProducer jmsProducer;
    private Session session;
    private Integer mappedPort;
    private Integer consolePort;

    @Before
    public void before() throws JMSException {
        Map<String, Object> senderProps = new HashMap<>();
        senderProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        senderProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        senderProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        kafkaProducer = new KafkaProducer<>(senderProps);

        mappedPort = mq.getMappedPort(1414);
        consolePort = mq.getMappedPort(9443);
        System.out.println("CONSOLE PORT " + consolePort);
        MQQueueConnectionFactory mqQueueConnectionFactory = getMqQueueConnectionFactory("localhost", mappedPort);
        session = mqQueueConnectionFactory.createConnection().createSession(false, AUTO_ACKNOWLEDGE);
        MQQueue mqQueue = new MQQueue("DEV.QUEUE.3");
        jmsProducer = session.createProducer(mqQueue);

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
    public void test() throws JMSException, InterruptedException {
        PCollection<String> output = createPipeline(p, testOptions, "localhost", mappedPort);
//        PAssert.that(output).containsInAnyOrder("jms1", "kafka1", "kafka2");
        PipelineResult result = p.run(testOptions);

//        jmsProducer.send(session.createTextMessage("jms1"));
//        jmsProducer.send(session.createTextMessage("jms2"));
//        jmsProducer.send(session.createTextMessage("jms3"));
//        jmsProducer.send(session.createTextMessage("jms4"));
        kafkaProducer.send(new ProducerRecord<>(TEST_TOPIC, "kafka1"));
        kafkaProducer.send(new ProducerRecord<>(TEST_TOPIC, "kafka2"));
        result.waitUntilFinish(Duration.millis(10000));
//        Thread.sleep(5000);
                jmsProducer.send(session.createTextMessage("jms1"));
        Thread.sleep(5000);

        MetricName elementsRead = SourceMetrics.elementsRead().getName();
        MetricQueryResults metrics = result.metrics().queryMetrics(
                MetricsFilter.builder()
//                        .addNameFilter(MetricNameFilter.named(elementsRead.getNamespace(), elementsRead.getName()))
                        .build());
        Iterable<MetricResult<Long>> counters = metrics.getCounters();
        // one source
//        Long committed = counters.iterator().next().getCommitted();
//        assertEquals(2, committed.longValue());
    }

    public interface TestOptions extends DirectOptions, KafkaConsumerOptions {
    }
}
