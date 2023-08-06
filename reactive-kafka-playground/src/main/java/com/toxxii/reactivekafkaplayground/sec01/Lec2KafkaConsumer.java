package com.toxxii.reactivekafkaplayground.sec01;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/*

    CONSUME MULTIPLE TOPICS

 */

public class Lec2KafkaConsumer {

    private static final Logger log = LoggerFactory.getLogger(Lec2KafkaConsumer.class);

    public static void main(String[] args) {
        //kafka.apache.org/documentation/
         var consumerConfig = Map.<String,Object>of(
                 ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                 ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                 ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                 ConsumerConfig.GROUP_ID_CONFIG, "inventory-service.group", //Consumer group
                 ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest",
                 ConsumerConfig.GROUP_INSTANCE_ID_CONFIG,"1"

        );
        var options = ReceiverOptions.create(consumerConfig)
                .subscription(Pattern.compile("order.*")); //Topico al que nos subscribimos
        KafkaReceiver.create(options)
                .receive()
                .doOnNext(r -> log.info("topic: {} key: {}, value: {}",r.topic(),r.key(),r.value()))
                .doOnNext(r -> r.receiverOffset().acknowledge())
                .subscribe();

    }

}
