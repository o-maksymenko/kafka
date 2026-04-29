package com.example.kafka.rest;

import com.example.kafka.kafka.producer.KafkaProducer;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;


@SpringBootTest
class KafkaControllerTest {

    @Autowired
    private KafkaController controller;

    @MockitoBean
    private KafkaProducer producer;

    @Test
    void sendMessage() {
        String message = "test";
        Mockito.doNothing().when(producer).sendMessage(Mockito.anyString());
        controller.sendMessage(message);
        Mockito.verify(producer, Mockito.atLeastOnce()).sendMessage(message);
    }
}