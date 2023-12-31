package com.toxxii.reactivekafkaplayground.kafkaTransaction;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;

import java.time.Duration;
import java.util.function.Predicate;

public class TransferEventProcessor {

    private static final Logger log = LoggerFactory.getLogger(TransferEventProcessor.class);
    private final KafkaSender<String,String> sender;

    public TransferEventProcessor(KafkaSender<String, String> sender) {
        this.sender = sender;
    }

    public Flux<SenderResult<String>> process(Flux<TransferEvent> flux){
        return flux.concatMap(this::validate)
                .concatMap(this::sendTransaction);
    }

    private Mono<SenderResult<String>> sendTransaction(TransferEvent event){
        var senderRecord = this.toSenderRecord(event);
        var manager = this.sender.transactionManager();
        //esto es lo que hace bajo escena this.sender.sendTransactionally()
        return manager.begin()
                .then(this.sender.send(senderRecord)
                        .concatWith(Mono.delay(Duration.ofSeconds(1)).then(Mono.fromRunnable(event.acknowledge()))) //delay for demo porpuses
                        .concatWith(manager.commit())
                        .last())
                .doOnError(ex -> log.error(ex.getMessage()))
                .onErrorResume(ex -> manager.abort());
    }

    //5 does not have money to transfer
    private Mono<TransferEvent> validate(TransferEvent event) {
        return Mono.just(event)
                .filter(Predicate.not(e -> e.key().equals("5"))) // Si no es 5 se procesa
                .switchIfEmpty(
                        Mono.<TransferEvent>fromRunnable(event.acknowledge())
                                .doFirst(() -> log.info("fails validation: {}",event.key()))
                ); //Si es vacio no iniciamos la transaccion solo hacemos el ack
    }

    private Flux<SenderRecord<String, String, String>> toSenderRecord(TransferEvent event) {
        var pr1 = new ProducerRecord<>("transaction-events",event.key(),"%s+%s".formatted(event.to(),event.amount()));
        var pr2 = new ProducerRecord<>("transaction-events",event.key(),"%s-%s".formatted(event.from(),event.amount()));
        var sr1 = SenderRecord.create(pr1,pr1.key());
        var sr2 = SenderRecord.create(pr2,pr2.key());
        return Flux.just(sr1,sr2);
    }

}
