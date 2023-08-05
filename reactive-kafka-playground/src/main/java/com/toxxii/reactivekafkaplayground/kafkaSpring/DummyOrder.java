package com.toxxii.reactivekafkaplayground.kafkaSpring;

public record DummyOrder(
    String orderid,
    String customerId
) {}
