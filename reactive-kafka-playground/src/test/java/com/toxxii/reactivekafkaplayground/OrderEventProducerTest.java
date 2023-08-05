package com.toxxii.reactivekafkaplayground;

import com.toxxii.reactivekafkaplayground.integrationTest.producer.OrderEvent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.test.StepVerifier;

import java.time.Duration;

@TestPropertySource(properties = "app=producer")
public class OrderEventProducerTest extends AbstractIT{

    private static final Logger log = LoggerFactory.getLogger(OrderEventProducerTest.class);

    @Test
    @DirtiesContext(methodMode = DirtiesContext.MethodMode.AFTER_METHOD) //Limpia el contexto de la aplicacion cuando termina
    public void producerTest() {
        KafkaReceiver<String, OrderEvent> receiver = createReceiver("order-event");
        var orderEvents = receiver.receive()
                .take(10)
                .doOnNext(r -> log.info("key: {}, value: {}", r.key(), r.value()));
        StepVerifier.create(orderEvents)
                .consumeNextWith(r-> Assertions.assertNotNull(r.value().orderID()))
                .expectNextCount(9) //Esperamos 9 eventos mas despues del primero
                .expectComplete() //Esperamos complete signal
                .verify(Duration.ofSeconds(10)); //No puede durar mas de 10s
    }

    @Test
    @DirtiesContext(methodMode = DirtiesContext.MethodMode.AFTER_METHOD) //Limpia el contexto de la aplicacion cuando termina
    public void producerTest2() {
        KafkaReceiver<String, OrderEvent> receiver = createReceiver("order-event");
        var orderEvents = receiver.receive()
                .take(10)
                .doOnNext(r -> log.info("key: {}, value: {}", r.key(), r.value()));
        StepVerifier.create(orderEvents)
                .consumeNextWith(r-> Assertions.assertNotNull(r.value().orderID()))
                .expectNextCount(9) //Esperamos 9 eventos mas despues del primero
                .expectComplete() //Esperamos complete signal
                .verify(Duration.ofSeconds(10)); //No puede durar mas de 10s
    }

}
