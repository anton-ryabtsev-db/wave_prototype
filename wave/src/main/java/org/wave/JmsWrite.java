package org.wave;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.checkerframework.checker.nullness.qual.EnsuresNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.RequiresNonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.util.LinkedList;
import java.util.List;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

public class JmsWrite extends PTransform<PCollection<String>, PCollection<String>> {
    private static final Logger LOG = LoggerFactory.getLogger(JmsWrite.class);

    @Nullable
    private final ConnectionFactory connectionFactory;
    @Nullable
    private final String queue;

    public static  JmsWrite jmsWrite () {
        return new Builder().build();
    }

    private JmsWrite(@Nullable ConnectionFactory connectionFactory, @Nullable String queue) {
        this.connectionFactory = connectionFactory;
        this.queue = queue;
    }

    public ConnectionFactory getConnectionFactory() {
        return connectionFactory;
    }

    public String getQueue() {
        return queue;
    }

    public JmsWrite withConnectionFactory(ConnectionFactory connectionFactory) {
        checkArgument(connectionFactory != null, "connectionFactory can not be null");
        return new Builder(this).setConnectionFactory(connectionFactory).build();
    }

    public JmsWrite withQueue(String queue) {
        checkArgument(queue != null, "queue can not be null");
        return new Builder(this).setQueue(queue).build();
    }

    @Override
    public PCollection<String> expand(PCollection<String> input) {
        return input.apply(ParDo.of(new Writer(this)));
    }

    private static class Writer extends DoFn<String, String> {
        private JmsWrite spec;

        private transient @Nullable List<String> accumulator;
        private Connection connection;
        private Session session;
        private MessageProducer producer;

        public Writer(JmsWrite spec) {
            this.spec = spec;
        }

        @Setup
        public void setup() throws Exception {
            if (producer == null) {
                this.connection = spec.getConnectionFactory().createConnection();
                this.connection.start();
                // false means we don't use JMS transaction.
                this.session = this.connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                Destination destination = session.createQueue(spec.getQueue());
                this.producer = this.session.createProducer(destination);
            }
        }

        @StartBundle
        @EnsuresNonNull({"accumulator"})
        public void startBundle() {
            LOG.info("startBundle");
            accumulator = new LinkedList<>();
        }

        @ProcessElement
        @RequiresNonNull({"accumulator"})
        public void processElement(ProcessContext context) {
            String e = context.element();
            LOG.info("process {}", e);
            accumulator.add(e);
            context.output(e);
        }


        @FinishBundle
        @RequiresNonNull({"accumulator"})
        public void outputAccumulators(FinishBundleContext context) throws JMSException {
            // Establish immutable non-null handles to demonstrate that calling other
            // methods cannot make them null
//            if (true) {
//                LOG.info("fail finish {}", accumulator.toString());
//                throw new RuntimeException("Exception on FinishBundle");
//            }
            final List<String> accumulator = this.accumulator;
            for (String element : accumulator) {
                TextMessage message = session.createTextMessage(element);
                producer.send(message);
            }
            LOG.info("finish {}", accumulator.toString());
            this.accumulator = null;
        }

        @Teardown
        public void teardown() throws Exception {
            producer.close();
            producer = null;
            session.close();
            session = null;
            connection.stop();
            connection.close();
            connection = null;
        }
    }

    static final class Builder {
        @Nullable
        private ConnectionFactory connectionFactory;
        @Nullable
        private String queue;

        Builder() {
        }

        private Builder(JmsWrite source) {
            this.connectionFactory = source.getConnectionFactory();
            this.queue = source.getQueue();
        }

        public Builder setConnectionFactory(ConnectionFactory connectionFactory) {
            this.connectionFactory = connectionFactory;
            return this;
        }

        public Builder setQueue(String queue) {
            this.queue = queue;
            return this;
        }

        JmsWrite build() {
            return new JmsWrite(connectionFactory, queue);
        }
    }
}
