package com.elgamal.redecide;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

@TestConfiguration
class TestHooksConfig {

    @Bean
    @Primary
    PausingHooks pausingHooks() {
        return new PausingHooks();
    }
}
