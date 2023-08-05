package com.toxxii.reactivekafkaplayground;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = "com.toxxii.reactivekafkaplayground.integrationTest.${app}")
public class ReactiveKafkaPlaygroundApplication {

	public static void main(String[] args) {
		SpringApplication.run(ReactiveKafkaPlaygroundApplication.class, args);
	}

}
