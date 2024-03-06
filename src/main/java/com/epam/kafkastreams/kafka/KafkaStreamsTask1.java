package com.epam.kafkastreams.kafka;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class KafkaStreamsTask1 {

    @Value("${kafka.topics.input-topic1}")
    private String inputTopic;

    @Value("${kafka.topics.output-topic1}")
    private String outputTopic;

    private final StreamsBuilder streamsBuilder;

    @PostConstruct
    public void kafkaStreams() {
        KStream<String, String> inputStream = streamsBuilder.stream(inputTopic);
        inputStream
                .peek((key, value) -> System.out.println("Received message: " + value))
                .to(outputTopic);
    }
}
