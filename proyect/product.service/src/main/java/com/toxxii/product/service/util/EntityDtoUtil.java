package com.toxxii.product.service.util;

import com.toxxii.product.service.dto.ProductDto;
import com.toxxii.product.service.entity.Product;
import org.springframework.beans.BeanUtils;

public class EntityDtoUtil {

    public static ProductDto toDto(Product product){
        var dto = new ProductDto();
        BeanUtils.copyProperties(product,dto); //In real life no se usa essto
        return dto;
    }

}
