package com.toxxii.reactivekafkaplayground.kafkaSpring;

import java.time.LocalDateTime;
import java.util.UUID;

public record OrderEvent (
  UUID orderID,
  long customerId,
  LocalDateTime orderData
){}
