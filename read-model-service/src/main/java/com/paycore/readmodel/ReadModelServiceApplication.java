package com.paycore.readmodel;

import com.paycore.common.clock.ClockProvider;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class ReadModelServiceApplication {
    public static void main(String[] args) {
        SpringApplication.run(ReadModelServiceApplication.class, args);
    }

    @Bean
    public ClockProvider clockProvider() {
        return ClockProvider.systemUtc();
    }
}
