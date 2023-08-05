package com.toxxii.reactivekafkaplayground.kafkaSpring;

import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.List;

@Configuration
public class KafkaConsumerConfig {

    @Bean
    public ReceiverOptions<String,DummyOrder> receiverOptions(KafkaProperties kafkaProperties){
        //kafkaProperties es el yaml que hemos creado
        return ReceiverOptions.<String,DummyOrder>create(kafkaProperties.buildConsumerProperties())
                .consumerProperty(JsonDeserializer.REMOVE_TYPE_INFO_HEADERS, "false") //Con esto no borramos la cabecera
                .consumerProperty(JsonDeserializer.USE_TYPE_INFO_HEADERS,false) //Con esto no usamos la info de las cabeceras
                .consumerProperty(JsonDeserializer.VALUE_DEFAULT_TYPE,DummyOrder.class)
                .subscription(List.of("order-event"));
    }

    @Bean
    public ReactiveKafkaConsumerTemplate<String,DummyOrder> consumerTemplate(ReceiverOptions<String,DummyOrder> options) {
        return new ReactiveKafkaConsumerTemplate<>(options);
    }

}
