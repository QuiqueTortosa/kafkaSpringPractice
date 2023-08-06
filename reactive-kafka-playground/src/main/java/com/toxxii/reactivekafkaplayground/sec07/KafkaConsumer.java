package com.toxxii.reactivekafkaplayground.sec07;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.List;
import java.util.Map;

/*
    goal: Kafka Fault Tolerance Demo Cluster
 */

public class KafkaConsumer {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumer.class);

    public static void main(String [] args) {
        //kafka.apache.org/documentation/
         var consumerConfig = Map.<String,Object>of(
                 ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:8081",
                 ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                 ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                 ConsumerConfig.GROUP_ID_CONFIG, "demo-group",
                 ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest",
                 ConsumerConfig.GROUP_INSTANCE_ID_CONFIG,1,
                 ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RangeAssignor.class.getName() //Por Defecto

         );
        var options = ReceiverOptions.create(consumerConfig)
                .subscription(List.of("order-events2")); //Topico al que nos subscribimos
        KafkaReceiver.create(options)
                .receive()
                .doOnNext(r -> log.info("key: {}, value: {}",r.key(),r.value()))
                .doOnNext(r -> r.receiverOffset().acknowledge()) //Con esto informas al broker que se ha procesado el mensaje
                .subscribe();

    }

}
