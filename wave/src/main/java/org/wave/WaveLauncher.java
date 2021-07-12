package org.wave;

import com.ibm.mq.jms.MQQueueConnectionFactory;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.jms.JmsIO;
import org.apache.beam.sdk.io.jms.JmsRecord;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.SinkMetrics;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

import static com.ibm.msg.client.jms.JmsConstants.PASSWORD;
import static com.ibm.msg.client.jms.JmsConstants.USERID;
import static com.ibm.msg.client.jms.JmsConstants.USER_AUTHENTICATION_MQCSP;
import static com.ibm.msg.client.wmq.common.CommonConstants.WMQ_CM_CLIENT;

public class WaveLauncher {
    private static final Logger LOG = LoggerFactory.getLogger(WaveLauncher.class);

    public static void main(String[] args) {
        KafkaConsumerOptions options = PipelineOptionsFactory.create().as(KafkaConsumerOptions.class);

        Pipeline p = Pipeline.create(options);
        //TODO
        createPipeline(p, options, "", 0);
    }

    public static PCollection<String> createPipeline(Pipeline p, KafkaConsumerOptions options, String host, Integer mappedPort) {
//        PCollection<String> kafkaSource = p.apply("KafkaSource", KafkaIO.<String, String>read()
//                .withBootstrapServers(options.getKafkaBootstrapServers())
//                .withTopic(options.getKafkaTopic())
//                .withKeyDeserializer(StringDeserializer.class)
//                .withValueDeserializer(StringDeserializer.class)
//                .updateConsumerProperties(ImmutableMap.of(
//                        ConsumerConfig.GROUP_ID_CONFIG, options.getKafkaConsumerGroup(),
//                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, options.getKafkaConsumerOffsetReset(),
//                        ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000"))
//                .commitOffsetsInFinalize()
//                .withoutMetadata())
//        .apply("KafkaPayload", MapElements.via(new GetKafkaPayloadFunction()));

        PCollection<Long> kafkaSource =
                p.apply(GenerateSequence.from(0).withRate(1, Duration.standardSeconds(3L)))
                .apply("Generated", MapElements.via(new SimpleFunction<Long, Long>() {
                    @Override
                    public Long apply(Long input) {
                        LOG.info("generated {}", input);
                        return input;
                    }
                }))
                .apply("FixedKafkaWindows", Window.into(FixedWindows.of(Duration.standardSeconds(1))));

        MQQueueConnectionFactory connectionFactory = getMqQueueConnectionFactory(host, mappedPort);
        PCollection<JmsRecord> jmsSource = p.apply(
                JmsIO.read().withConnectionFactory(connectionFactory).withQueue("DEV.QUEUE.3"));

        PCollectionView<List<String>> view = jmsSource
                .apply("GlobalJmsWindow",
                    Window.<JmsRecord>into(new GlobalWindows())
                            .triggering(Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane()))
                            .discardingFiredPanes())
                            .apply("Map view", ParDo.of(new DoFn<JmsRecord, JmsRecord>() {
                                @ProcessElement
                                public void process(@Timestamp Instant timestamp, ProcessContext ctx, BoundedWindow window) {
                                    LOG.info("WINDOWINFO {} {} {} {}", window.maxTimestamp(), ctx.pane(), ctx.element().getPayload(), timestamp);                        ctx.output(ctx.element());
                                }
                            }))
                .apply(Combine.globally(new InputCombineFn()))
                .apply(View.asList());

        final TupleTag<Long> sink1Tag = new TupleTag<>();
        final TupleTag<String> sink2Tag = new TupleTag<>();

        PCollectionTuple tuple = kafkaSource
                .apply(ParDo.of(new LogFunction(view, sink1Tag, sink2Tag))
                        .withSideInput("view input", view)
                        .withOutputTags(sink1Tag, TupleTagList.of(sink2Tag)));

        tuple.get(sink1Tag).apply(MapElements.via(new SimpleFunction<Long, String>() {
            @Override
            public String apply(Long input) {
                return input.toString();
            }
        }))
                .apply("JmsSink1", JmsWrite.jmsWrite()
                        .withConnectionFactory(connectionFactory)
                        .withQueue("DEV.QUEUE.1"));
        ;
        return tuple.get(sink2Tag)
                .setCoder(StringUtf8Coder.of())
                .apply("JmsSink2", JmsWrite.jmsWrite()
                .withConnectionFactory(connectionFactory)
                .withQueue("DEV.QUEUE.2"));
    }

    public static MQQueueConnectionFactory getMqQueueConnectionFactory(String host, int port) {
        MQQueueConnectionFactory connectionFactory = new MQQueueConnectionFactory();
        try {
            connectionFactory.setTransportType(WMQ_CM_CLIENT);
            connectionFactory.setStringProperty(USERID, "app");
            connectionFactory.setBooleanProperty(USER_AUTHENTICATION_MQCSP, true);
            connectionFactory.setStringProperty(PASSWORD, "passw0rd");

            connectionFactory.setHostName(host);
            connectionFactory.setPort(port);

            connectionFactory.setQueueManager("mq_queue");
            connectionFactory.setChannel("DEV.APP.SVRCONN");
        } catch (JMSException e) {
            e.printStackTrace();
        }
        return connectionFactory;
    }

    private static class InputCombineFn extends Combine.CombineFn<JmsRecord, InputCombineFn.Accum, String> {

        @Override
        public Accum createAccumulator() {
            return new Accum(null);
        }

        @Override
        public Accum addInput(Accum mutableAccumulator, JmsRecord input) {
            LOG.info("Accum add {} {}", mutableAccumulator,  input == null ? "null" : input.getPayload());
            mutableAccumulator.r = getLast(mutableAccumulator.r, input);
            return mutableAccumulator;
        }

        @Override
        public Accum mergeAccumulators(Iterable<Accum> accumulators) {
            JmsRecord r = null;
            for (Accum accumulator : accumulators) {
                r = getLast(accumulator.r, r);
            }
            LOG.info("Accum merge {} {}", accumulators, r == null ? "null" : r.getPayload());
            return new Accum(r);
        }

        @Override
        public String extractOutput(Accum accumulator) {
            LOG.info("Accum extract {}", accumulator);
            return accumulator.r == null ? defaultValue() : accumulator.r.getPayload();
        }

        @Override
        public String defaultValue() {
            return "Accum extract default";
        }

        private JmsRecord getLast(JmsRecord j1, JmsRecord j2) {
            if (j1 == null) {
                return j2;
            }
            if (j2 == null) {
                return j1;
            }
            return j1.getJmsTimestamp() > j2.getJmsTimestamp() ? j1 : j2;
        }

        class Accum implements Serializable {
            JmsRecord r;

            public Accum(JmsRecord r) {
                this.r = r;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                Accum accum = (Accum) o;
                return Objects.equals(r, accum.r);
            }

            @Override
            public int hashCode() {
                return Objects.hash(r);
            }

            @Override
            public String toString() {
                String rS = r == null ? null : (r.getPayload() + " " + r.getJmsTimestamp());
                return "Accum{" +
                        "r=" + rS +
                        '}';
            }
        }
    }

    private static class LogFunction extends DoFn<Long, Long> {
        private static final Logger LOG = LoggerFactory.getLogger(LogFunction.class);
        private final Counter elementsWritten = SinkMetrics.elementsWritten();
        private final PCollectionView<List<String>> view;
        private final TupleTag<Long> sink1Tag;
        private final TupleTag<String> sink2Tag;

        private static transient int count = 0;

        public LogFunction(PCollectionView<List<String>> view, TupleTag<Long> sink1Tag, TupleTag<String> sink2Tag) {
            this.view = view;
            this.sink1Tag = sink1Tag;
            this.sink2Tag = sink2Tag;
        }

        @ProcessElement
        public void apply(@Element Long input,
                          MultiOutputReceiver receiver, ProcessContext c) {
//            if (count++ %3 == 0) {
//                LOG.info("log failed {}", input);
//                throw new RuntimeException();
//            }
            List<String> sideInput = c.sideInput(view);
            LOG.info("log received {} side {}", input, sideInput);
            elementsWritten.inc();
            if (input % 2 == 1) {
                receiver.get(sink1Tag).output(input);
            } else {
                receiver.get(sink2Tag).output(input.toString());
            }
        }
    }

    private static class GetKafkaPayloadFunction extends SimpleFunction<KV<String, String>, String> {
        @Override
        public String apply(KV<String, String> input) {
            return input.getValue();
        }
    }

    private static class GetJmsPayloadFunction extends SimpleFunction<JmsRecord, JmsRecord> {
        @Override
        public JmsRecord apply(JmsRecord input) {
            LOG.info("received {}", input.getPayload());
            return input;
        }
    }
}
