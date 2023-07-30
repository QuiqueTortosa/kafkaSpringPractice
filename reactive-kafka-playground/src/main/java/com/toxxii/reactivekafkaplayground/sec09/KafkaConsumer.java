package com.toxxii.reactivekafkaplayground.sec09;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/*
    goal: FlatMap for paralleing process
 */

public class KafkaConsumer {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumer.class);

    public static void main(String [] args) {
        //kafka.apache.org/documentation/
         var consumerConfig = Map.<String,Object>of(
                 ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                 ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                 ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                 ConsumerConfig.GROUP_ID_CONFIG, "demo-group",
                 ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest",
                 ConsumerConfig.GROUP_INSTANCE_ID_CONFIG,"1",
                 ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 3

         );
        var options = ReceiverOptions.create(consumerConfig)
                .commitInterval(Duration.ofSeconds(1)) //Por defecto es 5 
                .subscription(List.of("order-events2")); //Topico al que nos subscribimos
        KafkaReceiver.create(options)
                .receiveAutoAck()
                .log()
                .flatMap(KafkaConsumer::batchProcess)
                .subscribe();
    }

    private static Mono<Void> batchProcess(Flux<ConsumerRecord<Object, Object>> flux) {
        return flux
                .publishOn(Schedulers.boundedElastic()) //Crea un pool de threads, hay mas schedulers
                .doFirst(() -> log.info("-----------------"))
                .doOnNext(r -> log.info("key: {}, value: {}",r.key(),r.value()))
                .then(Mono.delay(Duration.ofSeconds(1)))
                .then();
    }

}
