package com.epam.kafkastreams.kafka.Task4;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@RequiredArgsConstructor
public class KafkaStreamsTask4 {

    private final StreamsBuilder builder;

    @Bean
    public KStream<Integer, Employee> sendCustomData() {
        KStream<Integer, Employee> kStream = builder.stream("task4", Consumed.with(
                new Serdes.IntegerSerde(), new EmployeeSerde()));
        kStream.filter((k, v) -> v != null).foreach((k, v) -> System.out.printf("Data is: %s", v.toString()));
        return kStream;
    }
}
