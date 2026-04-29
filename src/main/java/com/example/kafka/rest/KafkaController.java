package com.example.kafka.rest;

import com.example.kafka.kafka.producer.KafkaProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/kafka")
public class KafkaController {
    @Autowired
    private KafkaProducer kafkaProducer;

    @GetMapping("/{message}")
    public void sendMessage(@PathVariable String message) {
        kafkaProducer.sendMessage(message);
    }
}
