package com.toxxii.reactivekafkaplayground.sec05;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.List;
import java.util.Map;

/*
    goal: Change partition assignment strategy

    Lo que hace este nueva estrategia, es no parar todos los cg como la anterior, si no que mira quien tiene mas y
    se lo da al que no tiene, (Cooperative sticky implementation).
    Con esto evitamos cambiar todos los consumers al reasignar.
 */

public class KafkaConsumer {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumer.class);

    public static void start(String instanceId) {
        //kafka.apache.org/documentation/
         var consumerConfig = Map.<String,Object>of(
                 ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                 ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                 ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                 ConsumerConfig.GROUP_ID_CONFIG, "demo-group-123",
                 ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest",
                 ConsumerConfig.GROUP_INSTANCE_ID_CONFIG,instanceId,
                 ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName() //Cambia la estrategia de asignamiento
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
