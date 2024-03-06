package com.epam.kafkastreams.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaStreamsTask2 {

    private final StreamsBuilder streamsBuilder;

    @Bean
    public Map<String, KStream<Integer, String>> kafkaStreams() {
        KStream<String, String> inputStream = streamsBuilder.stream("task2");
        return inputStream
                .filter((key, value) -> Objects.nonNull(value))
                .flatMapValues(value -> Arrays.asList(value.split("\\s+")))
                .map((key, value) -> new KeyValue<>(value.length(), value))
                .peek((key, value) -> log.info("Received message - Key: {}, Value: {}", key, value))
                .split(Named.as("words-"))
                .branch((key, value) -> value.length() < 10, Branched.as("short"))
                .branch((key, value) -> value.length() >= 10, Branched.as("long"))
                .noDefaultBranch();
    }

    @Bean
    public KStream<Integer, String> mergeKafkaStreams(Map<String, KStream<Integer, String>> kafkaStreams) {
        return kafkaStreams.values().stream()
                .reduce(KStream::merge)
                .orElseThrow(IllegalStateException::new)
                .filter((key, value) -> value.contains("a"))
                .peek((key, value) -> log.info("Filtered Message - Key: {}, Value with 'a': {}", key, value));
    }
}
