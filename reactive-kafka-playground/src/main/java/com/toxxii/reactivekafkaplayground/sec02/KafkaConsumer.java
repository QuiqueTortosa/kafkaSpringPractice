package com.toxxii.reactivekafkaplayground.sec02;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.List;
import java.util.Map;

/*
    goal: Product and consume 1 million events

    producer ----> kafka broker <-------> consumer
 */

public class KafkaConsumer {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumer.class);

    public static void main(String[] args) {
        //kafka.apache.org/documentation/
         var consumerConfig = Map.<String,Object>of(
                 ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                 ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                 ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                 ConsumerConfig.GROUP_ID_CONFIG, "demo-group-123", //Consumer group
                 ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest",
                 ConsumerConfig.GROUP_INSTANCE_ID_CONFIG,"1"

        );
        var options = ReceiverOptions.create(consumerConfig)
                .subscription(List.of("order-events")); //Topico al que nos subscribimos
        KafkaReceiver.create(options)
                .receive()
               // .take(3) //Solo procesara 3 eventos
                .doOnNext(r -> log.info("key: {}, value: {}",r.key(),r.value()))
                .doOnNext(r -> r.receiverOffset().acknowledge()) //Con esto informas al broker que se ha procesado el mensaje
                .subscribe();

    }

}
