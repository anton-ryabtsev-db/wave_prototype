package org.wave;

import com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WaveLauncher {
    private static final Logger LOG = LoggerFactory.getLogger(WaveLauncher.class);

    public static void main(String[] args) {
        KafkaConsumerOptions options = PipelineOptionsFactory.create().as(KafkaConsumerOptions.class);

        Pipeline p = Pipeline.create(options);
        createPipeline(p, options);
    }

    public static PCollection<KV<String, String>> createPipeline(Pipeline p, KafkaConsumerOptions options) {
        return p.apply("KafkaSource", KafkaIO.<String, String>read()
                .withBootstrapServers(options.getKafkaBootstrapServers())
                .withTopic(options.getKafkaTopic())
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(StringDeserializer.class)
                .updateConsumerProperties(ImmutableMap.of(
                        ConsumerConfig.GROUP_ID_CONFIG, options.getKafkaConsumerGroup(),
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, options.getKafkaConsumerOffsetReset(),
                        ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000"))
                .commitOffsetsInFinalize()
                .withoutMetadata()
        ).apply(MapElements.via(new KVSimpleFunction()));
    }

    private static class KVSimpleFunction extends SimpleFunction<KV<String, String>, KV<String, String>> {
        @Override
        public KV<String, String> apply(KV<String, String> input) {
            LOG.info("received {} {}", input.getKey(), input.getValue());
            return input;
        }
    }
}
