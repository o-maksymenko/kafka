package com.example.kafka.model;

public record MyMessage (String message, String description) {
    public MyMessage() {
        this("message","description");
    }
}
