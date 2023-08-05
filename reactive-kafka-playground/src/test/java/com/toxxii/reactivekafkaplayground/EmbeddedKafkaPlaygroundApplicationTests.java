package com.toxxii.reactivekafkaplayground;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.test.condition.EmbeddedKafkaCondition;
import org.springframework.kafka.test.context.EmbeddedKafka;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.List;
import java.util.Map;

@EmbeddedKafka(
        //ports = 9092, Esto no es una buena practica, por que en CICD alomejor hay varias ejecuciones en paralelo y habra conflicto de peurtos
        partitions = 1,
        brokerProperties = { "auto.create.topics.enable=false" },
        topics = { "order-events" }

)
class EmbeddedKafkaPlaygroundApplicationTests {

    @Test
    void embeddedKafkaDemo() {
        var brokers = EmbeddedKafkaCondition.getBroker().getBrokersAsString(); //Contiene la ip y el puerto del broker

		StepVerifier.create(Producer.run(brokers))
				.verifyComplete();
		StepVerifier.create(Consumer.run(brokers))
				.verifyComplete();
    }

    private static class Consumer {

        private static final Logger log = LoggerFactory.getLogger(Consumer.class);

        private static Mono<Void> run(String brokers) {
            var consumerConfig = Map.<String, Object>of(
                    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers,
                    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                    ConsumerConfig.GROUP_ID_CONFIG, "demo-group-123",
                    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                    ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "1" //Con esto asignamos un identificar unico al consumidor para evitar el reequilibrio al entrar y salir el consumer

            );
            var options = ReceiverOptions.create(consumerConfig)
                    .subscription(List.of("order-events"));
            return KafkaReceiver.create(options)
                    .receive()
                    .take(10) //Solo procesara 10 eventos
                    .doOnNext(r -> log.info("key: {}, value: {}", r.key(), r.value()))
                    .doOnNext(r -> r.receiverOffset().acknowledge())
                    .then();
        }
    }

    private static class Producer {

        private static final Logger log = LoggerFactory.getLogger(Producer.class);

        public static Mono<Void> run(String brokers) {
            var producerConfig = Map.<String, Object>of(
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers,
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
            );

            var options = SenderOptions.<String, String>create(producerConfig);

            var flux = Flux.range(1,10)
					.delayElements(Duration.ofMillis(100))
                    .map(i -> new ProducerRecord<>("order-events", i.toString(), "order-" + i))
                    .map(pr -> SenderRecord.create(pr, pr.key())); //le mandamos pr.key() para saber cuantos eventos le hemos mandado

            var sender = KafkaSender.create(options);
            return sender
                    .send(flux) //key,value,resultType
                    .doOnNext(result -> log.info("correlation id: {}", result.correlationMetadata())) //correlationMetadata es pr.key() del flux, esto se hace para ponerlo en el log etc para saber que eventos se enviaron correctamente
                    .doOnComplete(sender::close)
                    .then();
        }
    }
}
