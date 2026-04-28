package com.example.kafka.rest;

import com.example.kafka.kafka.KafkaProducerImpl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.concurrent.CompletableFuture;

import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class KafkaProducerTest {
    @InjectMocks
    private KafkaProducerImpl producer;

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    @Mock
    CompletableFuture<SendResult<String, String>> future;

    @Test
    void sendMessage() {
        when(kafkaTemplate.send(Mockito.anyString(), Mockito.anyString()))
                .thenReturn(future);

        String message = "test";
        producer.sendMessage(message);
        Mockito.verify(kafkaTemplate, Mockito.times(1)).send("my_topic2",
                message);
    }
}