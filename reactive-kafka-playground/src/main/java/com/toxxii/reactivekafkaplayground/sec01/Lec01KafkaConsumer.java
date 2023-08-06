package com.toxxii.reactivekafkaplayground.sec01;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.List;
import java.util.Map;

/*
    goal: to demo a simple kafka consumer using reactor kafka

    producer ----> kafka broker <-------> consumer
 */

public class Lec01KafkaConsumer {

    private static final Logger log = LoggerFactory.getLogger(Lec01KafkaConsumer.class);

    public static void main(String[] args) {
        //kafka.apache.org/documentation/
         var consumerConfig = Map.<String,Object>of(
                 ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                 ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                 ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                 ConsumerConfig.GROUP_ID_CONFIG, "demo-group-123", //Consumer group
                 ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest", //Para recibir los mensajes desde el principio, esto tardar 45s
                 /*
                    Ya hay 1 CG asignado previamente al cual le envie todos los mensajes y ahora te estas intentando unir
                    El broker no esta seguro si puede asignar la particion al nuevo CG, el broker va a esperar el tiempo
                    que este asignado en la propiedad session.timeout.ms
                 */
                 ConsumerConfig.GROUP_INSTANCE_ID_CONFIG,"1" //Con esto asignamos un identificar unico al consumidor para evitar el reequilibrio al entrar y salir el consumer

        );
        var options = ReceiverOptions.create(consumerConfig)
                .subscription(List.of("order-events")); //Topico al que nos subscribimos
        KafkaReceiver.create(options)
                .receive()
                .take(3) //Solo procesara 3 eventos
                .doOnNext(r -> log.info("key: {}, value: {}",r.key(),r.value()))
                .doOnNext(r -> r.receiverOffset().acknowledge()) //Con esto informas al broker que se ha procesado el mensaje
                .subscribe();

    }

}
