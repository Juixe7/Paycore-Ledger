package com.paycore.payments;

import com.paycore.common.clock.ClockProvider;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class PaymentsCoreApplication {

    public static void main(String[] args) {
        org.springframework.boot.SpringApplication.run(PaymentsCoreApplication.class, args);
    }

    @Bean
    public ClockProvider clockProvider() {
        return ClockProvider.systemUtc();
    }
}
