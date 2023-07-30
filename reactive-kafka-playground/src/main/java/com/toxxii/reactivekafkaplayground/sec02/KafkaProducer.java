package com.toxxii.reactivekafkaplayground.sec02;

import com.toxxii.reactivekafkaplayground.sec01.Lec2KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

import java.time.Duration;
import java.util.Map;

public class KafkaProducer {

    private static final Logger log = LoggerFactory.getLogger(KafkaProducer.class);

    public static void main(String[] args) {
        var producerConfig = Map.<String, Object>of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
        );

        var options = SenderOptions.<String, String>create(producerConfig)
                .maxInFlight(10000); //MaxInFlight es el numero de eventos del que puede hacer prefetch
                                        //Esto por defecto es 256, si lo aumentamos a 10000 es mas rapido.
                                        // Hay que tener cuidado con esto, por que puede dar un error de memoria al tener tantos
                                        // eventos en memoria.

        var flux = Flux.range(1,1000000)
                .map(i -> new ProducerRecord<>("order-events", i.toString(), "order-" + i))
                .map(pr -> SenderRecord.create(pr, pr.key())); //le mandamos pr.key() para saber cuantos eventos le hemos mandado

        var start = System.currentTimeMillis();
        var sender = KafkaSender.create(options);
        sender
                .send(flux) //key,value,resultType
                .doOnNext(result -> log.info("correlation id: {}", result.correlationMetadata())) //correlationMetadata es pr.key() del flux, esto se hace para ponerlo en el log etc para saber que eventos se enviaron correctamente
                .doOnComplete(() -> {
                    log.info("Total time taken: {} ms", (System.currentTimeMillis() - start));
                })
                .subscribe();
    }

}
