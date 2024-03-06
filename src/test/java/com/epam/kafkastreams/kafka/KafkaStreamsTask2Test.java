package com.epam.kafkastreams.kafka;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
public class KafkaStreamsTask2Test {

    private static final String INPUT_TOPIC_NAME = "task2";
    private static final String OUTPUT_TOPIC_NAME = "output-topic";
    private Properties props;
    private StreamsBuilder builder;
    private KafkaStreamsTask2 kafkaStreamsTask2;

    @BeforeEach
    public void setUp() {
        props = createProperties();
        builder = new StreamsBuilder();
        kafkaStreamsTask2 = new KafkaStreamsTask2(builder);
    }

    @Test
    public void testKafkaStreamsTask2() {
        Map<String, KStream<Integer, String>> streams = kafkaStreamsTask2.kafkaStreams();
        KStream<Integer, String> result = kafkaStreamsTask2.mergeKafkaStreams(streams);
        result.to(OUTPUT_TOPIC_NAME);

        try (final TopologyTestDriver testDriver = new TopologyTestDriver(builder.build(), props)) {
            TestInputTopic<String, String> inputTopic =
                    testDriver.createInputTopic(INPUT_TOPIC_NAME, Serdes.String().serializer(), Serdes.String().serializer());

            TestOutputTopic<Integer, String> outputTopic =
                    testDriver.createOutputTopic(OUTPUT_TOPIC_NAME, Serdes.Integer().deserializer(), Serdes.String().deserializer());

            String sentence = "abcd a1234 a0 b7";
            inputTopic.pipeInput(null, sentence);

            assertEquals("abcd", outputTopic.readValue());
            assertEquals("a1234", outputTopic.readValue());
            assertEquals("a0", outputTopic.readValue());
            assertTrue(outputTopic.isEmpty());
        }
    }

    private Properties createProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-application-id");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
        props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        return props;
    }
}
