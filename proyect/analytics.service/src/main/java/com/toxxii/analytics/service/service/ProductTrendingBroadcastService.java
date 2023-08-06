package com.toxxii.analytics.service.service;

import com.toxxii.analytics.service.dto.ProductTrendingDto;
import com.toxxii.analytics.service.repository.ProductViewRepository;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.List;
import java.util.function.Predicate;

@Service
@RequiredArgsConstructor
public class ProductTrendingBroadcastService {

    private final ProductViewRepository repository;
    private Flux<List<ProductTrendingDto>> trends;

    @PostConstruct
    private void init(){
        this.trends = this.repository.findTop5ByOrderByCountDesc()
                .map(pvc -> new ProductTrendingDto(pvc.getId(), pvc.getCount()))
                .collectList()
                //Esto no deberia estar vacio
                .filter(Predicate.not(List::isEmpty))
                //Se repite cada 3s
                .repeatWhen(l -> l.delayElements(Duration.ofSeconds(3)))
                //Lanzalo si hay algun cambio respecto la ultima vez
                .distinctUntilChanged()
                .cache(1); //Con esto lo cacheamos y solo tenemos 1 instancia para todos los usuarios
    }

    public Flux<List<ProductTrendingDto>> getTrends(){
        return this.trends;
    }

}
