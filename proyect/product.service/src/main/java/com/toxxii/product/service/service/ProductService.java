package com.toxxii.product.service.service;

import com.toxxii.product.service.dto.ProductDto;
import com.toxxii.product.service.event.ProductViewEvent;
import com.toxxii.product.service.repository.ProductRepository;
import com.toxxii.product.service.util.EntityDtoUtil;
import lombok.AllArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
@AllArgsConstructor
public class ProductService {

    private final ProductRepository productRepository;
    private final ProductViewEventProducer productViewEventProducer;

    public Mono<ProductDto> getProduct(int id) {
        return this.productRepository.findById(id)
                .doOnNext(e -> this.productViewEventProducer.emitEvent(new ProductViewEvent(e.getId())))
                .map(EntityDtoUtil::toDto);
    }

}
