package com.paycore.webhook;

import com.paycore.common.clock.ClockProvider;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class WebhookServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(WebhookServiceApplication.class, args);
    }

    @Bean
    public ClockProvider clockProvider() {
        return ClockProvider.systemUtc();
    }
}
