package com.toxxii.reactivekafkaplayground.kafkaTransaction;

public record TransferEvent (
        String key,
        String from,
        String to,
        String amount,
        Runnable acknowledge
        ){
}
