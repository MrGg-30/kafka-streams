package com.epam.kafkastreams.kafka;

import com.epam.kafkastreams.kafka.Task4.Employee;
import com.epam.kafkastreams.kafka.Task4.EmployeeDeserializer;
import com.epam.kafkastreams.kafka.Task4.EmployeeSerializer;
import com.epam.kafkastreams.kafka.Task4.KafkaStreamsTask4;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
public class KafkaStreamsTask4Test {

    private static final String INPUT_TOPIC_NAME = "task4";
    private static final String OUTPUT_TOPIC_NAME = "output-topic";
    private Properties props;
    private StreamsBuilder builder;
    private KafkaStreamsTask4 kafkaStreamsTask4;

    @BeforeEach
    public void setUp() {
        props = createProperties();
        builder = new StreamsBuilder();
        kafkaStreamsTask4 = new KafkaStreamsTask4(builder);
    }

    @Test
    public void testKafkaStreamsTask4() {
        KStream<Integer, Employee> integerEmployeeKStream = kafkaStreamsTask4.sendCustomData();
        integerEmployeeKStream.to(OUTPUT_TOPIC_NAME);

        try (final TopologyTestDriver testDriver = new TopologyTestDriver(builder.build(), props)) {
            TestInputTopic<String, Employee> inputTopic =
                    testDriver.createInputTopic(INPUT_TOPIC_NAME, Serdes.String().serializer(), new EmployeeSerializer());

            TestOutputTopic<String, Employee> outputTopic =
                    testDriver.createOutputTopic(OUTPUT_TOPIC_NAME, Serdes.String().deserializer(), new EmployeeDeserializer());

            Employee employee = new Employee("Tornike", "company", "position", 10000);

            inputTopic.pipeInput(null, employee);
            Employee result = outputTopic.readValue();
            assertEquals(employee, result);
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
