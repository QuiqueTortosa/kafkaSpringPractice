package com.toxxii.reactivekafkaplayground.kafkaSpring;

import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.SenderOptions;

import java.util.List;

@Configuration
public class KafkaProducerConfig {

    @Bean
    public SenderOptions<String,OrderEvent> senderOptions(KafkaProperties kafkaProperties){
        //kafkaProperties es el yaml que hemos creado
        return SenderOptions.<String,OrderEvent>create(kafkaProperties.buildProducerProperties());
    }

    @Bean
    public ReactiveKafkaProducerTemplate<String,OrderEvent> producerTemplate(SenderOptions<String,OrderEvent> options) {
        return new ReactiveKafkaProducerTemplate<>(options);
    }

}
