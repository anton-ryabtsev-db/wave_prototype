package org.wave;

import com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.util.Map;
import java.util.concurrent.ExecutionException;

@RunWith(JUnit4.class)
public class WaveLauncherTest {
    private static final Logger LOG = LoggerFactory.getLogger(WaveLauncherTest.class);

    private static final String TEST_TOPIC = "kafka-test";

    @ClassRule
    public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(1, true, 1, TEST_TOPIC);
    public KafkaTemplate<String, String> template;

    @Before
    public void before() {
        System.setProperty("spring.kafka.bootstrap-servers", embeddedKafka.getBrokersAsString());
        Map<String, Object> senderProps = KafkaTestUtils.senderProps(embeddedKafka.getBrokersAsString());
        senderProps.put("key.serializer", StringSerializer.class);
        ProducerFactory<String, String> producerFactory = new DefaultKafkaProducerFactory<>(senderProps);
        template = new KafkaTemplate<>(producerFactory);
        template.setDefaultTopic(TEST_TOPIC);
    }

    @After
    public void after() {
    }


    @Test
    public void test() throws ExecutionException, InterruptedException {
        template.sendDefault(TEST_TOPIC, "lolo");

        Pipeline pipeline = createPipeline();
        pipeline.run();

        template.sendDefault(TEST_TOPIC, "lolo");
    }


    public interface Options extends PipelineOptions {
        ValueProvider<String> getBootstrapServers();

        void setBootstrapServers(ValueProvider<String> value);

        ValueProvider<String> getInputTopic();

        void setInputTopic(ValueProvider<String> value);
    }

    private Pipeline createPipeline() {
//        String args = "--bootstrapServers=my_host:9092,inputTopic=kafka-test";
//        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        // Create the Pipeline object with the options we defined above.
        Pipeline p = Pipeline.create();
        p.apply(KafkaIO.<String, String>read()
                        .withBootstrapServers(embeddedKafka.getBrokersAsString())
                        .withTopic(TEST_TOPIC)
                        .withKeyDeserializer(StringDeserializer.class)
                        .withValueDeserializer(StringDeserializer.class)
                       .commitOffsetsInFinalize()
                       .updateConsumerProperties(ImmutableMap.of("group.id", "my_beam_app_1", "auto.offset.reset", "earliest"))

//                .updateConsumerProperties(ImmutableMap.of("auto.offset.reset", (Object)"earliest"))

                        // We're writing to a file, which does not support unbounded data sources. This line makes it bounded to
                        // the first 5 records.
                        // In reality, we would likely be writing to a data source that supports unbounded data, such as BigQuery.
                        .withMaxNumRecords(5)

                        .withoutMetadata() // PCollection<KV<Long, String>>
        )
                .apply("FormatResults", MapElements.via(new KVStringSimpleFunction()))
                .apply(TextIO.write().to("wordcounts"));
        return p;
    }

    private static class KVStringSimpleFunction extends SimpleFunction<KV<String, String>, String> {
        @Override
        public String apply(KV<String, String> input) {
            LOG.info("received {}", input.getValue());
            return input.getKey() + ": " + input.getValue();
        }
    }
}
