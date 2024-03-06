package com.epam.kafkastreams.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Service;

import java.time.Duration;


@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaStreamsTask3 {

    @Value("${kafka.topics.input-topic3-1}")
    private String inputTopic1;

    @Value("${kafka.topics.input-topic3-2}")
    private String inputTopic2;

    private final StreamsBuilder streamsBuilder;

    @Bean
    public void processMessage() {
        KStream<Long, String> stream1 = processStream(streamsBuilder.stream(inputTopic1));
        KStream<Long, String> stream2 = processStream(streamsBuilder.stream(inputTopic2));
        stream1
                .join(
                        stream2,
                        (leftValue, rightValue) -> leftValue + " = " + rightValue,
                        JoinWindows.ofTimeDifferenceAndGrace(Duration.ofMinutes(1), Duration.ofSeconds(30)),
                        StreamJoined.with(Serdes.Long(), Serdes.String(), Serdes.String()))
                .peek((key, value) -> System.out.println("Joined message: Key=" + key + ", Value=" + value));
    }

    private KStream<Long, String> processStream(KStream<String, String> stream) {
        return stream
                .filter((key, value) -> value != null && value.contains(":"))
                .map((key, value) -> {
                    Long newKey = extractNumberFromValue(value);
                    return new KeyValue<>(newKey, value);
                })
                .peek((key, value) -> log.info("Received message - Key: {}, Value: {}", key, value));
    }

    private Long extractNumberFromValue(String value) {
        String[] parts = value.split(":");
        return Long.parseLong(parts[0]);
    }
}
