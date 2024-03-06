package com.epam.kafkastreams.kafka.Task4;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

public class EmployeeSerde implements Serde<Employee> {

    @Override
    public Serializer<Employee> serializer() {
        return new EmployeeSerializer();
    }

    @Override
    public Deserializer<Employee> deserializer() {
        return new EmployeeDeserializer();
    }
}
