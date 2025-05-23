package io.lettuce.scenario;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.TestTags;
import io.lettuce.scenario.workload.AsyncWorkloadGenerator;
import io.lettuce.scenario.workload.WorkloadResult;

@Tag(TestTags.SCENARIO_TEST)
public class AsyncWorkloadWithFaultInjectionTest {

    private static final Logger log = LoggerFactory.getLogger(AsyncWorkloadWithFaultInjectionTest.class);
    private static final String FAULT_INJECTOR_HOST = "localhost";
    private static final int FAULT_INJECTOR_PORT = 8080;

    @Test
    void shouldHandleConnectionInterruptionDuringWorkload() throws Exception {
        // Setup client with auto-reconnect
        RedisClient client = RedisClient.create();
        client.setOptions(ClientOptions.builder()
                .autoReconnect(true)
                .build());

        RedisURI redisUri = RedisURI.builder()
                .withHost(FAULT_INJECTOR_HOST)
                .withPort(FAULT_INJECTOR_PORT)
                .withTimeout(Duration.ofSeconds(5))
                .build();

        // Connect and create workload generator
        StatefulRedisConnection<String, String> connection = client.connect(redisUri);
        AsyncWorkloadGenerator workload = new AsyncWorkloadGenerator(connection);

        try {
            // Start workload
            workload.start().get();
            
            // Let it run for a bit before injection
            Thread.sleep(1000);

            // Inject fault (disconnect)
            log.info("Injecting network disconnect");
            // TODO: Implement actual fault injection using the framework
            
            // Let it run during the fault
            Thread.sleep(2000);

            // Stop workload and get results
            WorkloadResult result = workload.stop().thenCompose(v -> 
                CompletableFuture.completedFuture(new WorkloadResult(
                    Duration.ofMillis(3000), 
                    workload.getStats()
                ))
            ).get();

            // Log the results
            log.info("Workload completed: {}", result);
            log.info("Error rate: {}", result.getStats().getErrorRate());
            result.getStats().getErrors().forEach(error -> 
                log.error("Operation error:", error)
            );

            // We expect some operations to succeed and some to fail
            assertThat(result.getStats().getOperationsCompleted()).isPositive();
            assertThat(result.getStats().getOperationsFailed()).isPositive();
        } finally {
            connection.close();
            client.shutdown();
        }
    }
} 