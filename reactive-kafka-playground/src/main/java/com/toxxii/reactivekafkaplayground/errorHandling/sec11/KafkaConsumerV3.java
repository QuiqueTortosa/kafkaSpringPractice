package com.toxxii.reactivekafkaplayground.errorHandling.sec11;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/*
    goal: Retry depending on error
    En caso de que sea error de DB se cerrara la comunicacion con kafka ya que se propagara al receiverPipeline
    En caso de ser un OutOfBoundException seguira la ejecucion y se hara el acknowledge
 */

public class KafkaConsumerV3 {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerV3.class);

    public static void main(String [] args) {
        //kafka.apache.org/documentation/
         var consumerConfig = Map.<String,Object>of(
                 ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                 ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                 ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                 ConsumerConfig.GROUP_ID_CONFIG, "demo-group",
                 ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest",
                 ConsumerConfig.GROUP_INSTANCE_ID_CONFIG,"1"

         );
        var options = ReceiverOptions.create(consumerConfig)
                .subscription(List.of("order-events2")); //Topico al que nos subscribimos

        KafkaReceiver.create(options)
                .receive()
                .log()
                .concatMap(KafkaConsumerV3::process)
                .subscribe();
    }

    private static Mono<Void> process(ReceiverRecord<Object,Object> receiverRecord) {
        return Mono.just(receiverRecord)
                .doOnNext(r-> {
                    if(r.key().toString().equals("5")) throw new RuntimeException("DB is down");
                    var index = ThreadLocalRandom.current().nextInt(1,20);
                    log.info("key: {}, index: {} value: {}", r.key(),index,r.value().toString().toCharArray()[index]); //Con esto dara error solo en algunas ocasiones
                    r.receiverOffset().acknowledge();
                })
                .retryWhen(retrySpec())
                .doOnError(ex -> log.error(ex.getMessage()))
                //Si lo hemos intentado 3 veces, nos sigue dando el error de IndexOut... damos el acknowledge:
                .onErrorResume(IndexOutOfBoundsException.class, ex -> Mono.fromRunnable(()->receiverRecord.receiverOffset().acknowledge()))
                .then();
    }

    private static Retry retrySpec(){
        return Retry.fixedDelay(3, Duration.ofSeconds(1)) //Repite cada 1s 3 veces
                .filter(IndexOutOfBoundsException.class::isInstance) //Filtra por error
                .onRetryExhaustedThrow((spec,signal) -> signal.failure()); // muestra el mensaje de error original en caso de fallo
    }

}
