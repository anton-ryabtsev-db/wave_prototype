package org.wave;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation;

public interface KafkaConsumerOptions extends PipelineOptions, StreamingOptions {
    @Description("The kafka bootstrap servers")
    @Validation.Required
    String getKafkaBootstrapServers();

    void setKafkaBootstrapServers(String value);

    @Description("The Kafka topic")
    @Validation.Required
    String getKafkaTopic();

    void setKafkaTopic(String value);

    @Description("The Kafka consumer group")
    @Validation.Required
    String getKafkaConsumerGroup();

    void setKafkaConsumerGroup(String value);

    @Description("Offset reset: earliest or latest")
    @Validation.Required
    String getKafkaConsumerOffsetReset();

    void setKafkaConsumerOffsetReset(String value);
}
