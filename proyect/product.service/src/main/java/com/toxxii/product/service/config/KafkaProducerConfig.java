package com.toxxii.product.service.config;

import com.toxxii.product.service.event.ProductViewEvent;
import com.toxxii.product.service.service.ProductViewEventProducer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import reactor.core.publisher.Sinks;
import reactor.kafka.sender.SenderOptions;

@Configuration
public class KafkaProducerConfig {

    @Bean
    public SenderOptions<String, ProductViewEvent> senderOptions(KafkaProperties properties){
        return SenderOptions.create(properties.buildProducerProperties()); //Saca las propiedades del .yaml
    }

    @Bean
    public ReactiveKafkaProducerTemplate<String, ProductViewEvent> producerTemplate(SenderOptions<String, ProductViewEvent> senderOptions){
        return new ReactiveKafkaProducerTemplate<>(senderOptions);
    }

    @Bean
    public ProductViewEventProducer productViewEventProducer(ReactiveKafkaProducerTemplate<String, ProductViewEvent> template){
        //Unicast debido a que solo tenemos 1 suscriptor
        var sink = Sinks.many().unicast().<ProductViewEvent>onBackpressureBuffer();
        var flux = sink.asFlux();
        var eventProducer = new ProductViewEventProducer(template,sink,flux,"product-view-events");
        eventProducer.subscribe();
        return eventProducer;
    }

}
