package com.toxxii.reactivekafkaplayground.integrationTest.consumer;

public record DummyOrder(
    String orderID,
    String customerId
) {}
