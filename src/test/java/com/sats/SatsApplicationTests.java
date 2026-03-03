package com.sats;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

@SpringBootTest
@TestPropertySource(properties = {
        "sats.schema.registry-url=http://localhost:8081",
        "sats.dlq.topic=test-dlq",
        "sats.consumer.topics=test-topic",
        "spring.kafka.bootstrap-servers=localhost:9092"
})
class SatsApplicationTests {

    @Test
    void contextLoads() {
        // Verifies Spring context wires all beans without circular dependencies
    }
}
