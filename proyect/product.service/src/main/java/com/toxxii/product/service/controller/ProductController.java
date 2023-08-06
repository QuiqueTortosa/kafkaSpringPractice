package com.toxxii.product.service.controller;

import com.toxxii.product.service.dto.ProductDto;
import com.toxxii.product.service.service.ProductService;
import lombok.AllArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
@AllArgsConstructor
@RequestMapping("product")
public class ProductController {

    private final ProductService productService;

    @GetMapping("{productId}")
    public Mono<ResponseEntity<ProductDto>> view(@PathVariable Integer productId){
        return this.productService.getProduct(productId)
                .map(ResponseEntity::ok)
                .defaultIfEmpty(ResponseEntity.notFound().build());
    }

}
