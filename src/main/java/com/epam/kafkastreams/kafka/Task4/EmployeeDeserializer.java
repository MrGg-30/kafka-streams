package com.epam.kafkastreams.kafka.Task4;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

public class EmployeeDeserializer implements Deserializer<Employee> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Employee deserialize(String topic, byte[] data) {
        try {
            if (data == null){
                return null;
            }
            return objectMapper.readValue(new String(data, "UTF-8"), Employee.class);
        } catch (Exception e) {
            throw new SerializationException("Error when deserializing byte[] to Employee");
        }
    }
}
