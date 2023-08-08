package com.toxxii.analytics.service.service;

import com.toxxii.analytics.service.entity.ProductViewCount;
import com.toxxii.analytics.service.event.ProductViewEvent;
import com.toxxii.analytics.service.repository.ProductViewRepository;
import jakarta.annotation.PostConstruct;
import lombok.AllArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.kafka.receiver.ReceiverRecord;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
@AllArgsConstructor
public class ProductViewEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(ProductViewEventConsumer.class);
    private final ReactiveKafkaConsumerTemplate<String, ProductViewEvent> template;
    private final ProductViewRepository repository;
    private final Sinks.Many<Integer> sink = Sinks.many().unicast().onBackpressureBuffer();
    private final Flux<Integer> flux = sink.asFlux();

    @PostConstruct
    public void subscribe(){
        this.template
                .receive()
                //Si llegan 1000 mensaje o pasa 1 segundo pasa los items
                //Esto hace que se añada en batches a la bdd, aumentado el throughput
                .bufferTimeout(1000, Duration.ofSeconds(1))
                .flatMap(this::process)
                .subscribe();
    }

    public Flux<Integer> companionFlux(){
        return this.flux;
    }

    private Mono<Void> process(List<ReceiverRecord<String,ProductViewEvent>> events){
        var eventsMap = events.stream()
                .map(r -> r.value().getProductId())
                .collect(Collectors.groupingBy(
                        Function.identity(),
                        Collectors.counting()
                ));
        return this.repository.findAllById(eventsMap.keySet())
                .collectMap(ProductViewCount::getId)
                .defaultIfEmpty(Collections.emptyMap())
                .map(dbMap -> eventsMap.keySet().stream().map(productId -> updateViewCount(dbMap,eventsMap,productId)).collect(Collectors.toList()))
                .flatMapMany(list -> this.repository.saveAll(list))
                .doOnComplete(() -> events.get(events.size()-1).receiverOffset().acknowledge()) //Con hacer el ack del ultimo mensaje es suficiente
                .doOnComplete(() -> sink.tryEmitNext(1)) // companionFlux, el valor no importa es solo para notificar que algo se ha procesado
                .doOnError(ex -> log.error(ex.getMessage()))
                .then();
        /*
        {
            2: 5,
            10: 172,
            8: 120
        }
        db
        {
            10: 3,
            8: 2
        }
        update
        {
            2: 5,
            10: 175,
            8: 122
        }
         */
    }

    private ProductViewCount updateViewCount(Map<Integer, ProductViewCount> dbMap, Map<Integer,Long> eventMap, int productId){
        ProductViewCount productViewCount = dbMap.getOrDefault(productId, new ProductViewCount(productId,0L,true)); //Si el mapa es vacio creamos uno por defecto
        productViewCount.setCount(productViewCount.getCount() + eventMap.get(productId));
        return productViewCount;
    }

}
