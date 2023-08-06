package com.toxxii.reactivekafkaplayground.errorHandling.sec13;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.List;
import java.util.Map;

/*
    goal: poison pill messages demo
 */

public class KafkaConsumer {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumer.class);

    public static void main(String [] args) {
        //kafka.apache.org/documentation/
         var consumerConfig = Map.<String,Object>of(
                 ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                 ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
               //  ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class,
                 ConsumerConfig.GROUP_ID_CONFIG, "demo-group",
                 ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest",
                 ConsumerConfig.GROUP_INSTANCE_ID_CONFIG,"1"

         );
        var options = ReceiverOptions.<String,Integer>create(consumerConfig)
                .withValueDeserializer(errorHandlingDeserializer())
                .subscription(List.of("order-events5")); //Topico al que nos subscribimos

        KafkaReceiver.create(options)
                .receive()
                .doOnNext(r -> log.info("key: {}, value: {}",r.key(),r.value()))
                .doOnNext(r -> r.receiverOffset().acknowledge()) //Con esto informas al broker que se ha procesado el mensaje
                .subscribe();

    }

    private static ErrorHandlingDeserializer<Integer> errorHandlingDeserializer() {
        var deserializer = new ErrorHandlingDeserializer<>(new IntegerDeserializer());
        deserializer.setFailedDeserializationFunction(
                info -> {
                    log.error("failed records: {}", new String(info.getData()));
                    return -10000; //Si falla devuelve esto
                }
        );
        return deserializer;
    }

}
