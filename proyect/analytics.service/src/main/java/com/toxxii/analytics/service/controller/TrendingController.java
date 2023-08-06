package com.toxxii.analytics.service.controller;

import com.toxxii.analytics.service.dto.ProductTrendingDto;
import lombok.AllArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

import java.util.List;

@RestController
@AllArgsConstructor
@RequestMapping("trending")
public class TrendingController {

    @GetMapping(produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<List<ProductTrendingDto>> trending(){
        return Flux.empty();
    }

}
