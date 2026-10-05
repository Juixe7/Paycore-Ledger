package com.paycore.reconciliation;

import com.paycore.common.clock.ClockProvider;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class ReconciliationApplication {
    public static void main(String[] args) {
        SpringApplication.run(ReconciliationApplication.class, args);
    }

    @Bean
    public ClockProvider clockProvider() {
        return ClockProvider.systemUtc();
    }
}
