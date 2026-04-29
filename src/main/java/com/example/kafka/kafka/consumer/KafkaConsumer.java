package com.example.kafka.kafka.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaConsumer {

    @KafkaListener(topics = "${spring.kafka.topic}")
    public void processMessage(String message) {
        System.out.println("Received message: " + message);
    }
}
