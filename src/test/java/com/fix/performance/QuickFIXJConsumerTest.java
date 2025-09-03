package com.fix.performance;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import com.fix.performance.fix.FixMessageBuilder;
import com.fix.performance.queue.ChronicleQueueService;

public class QuickFIXJConsumerTest {
    private Path tempDir;

    @AfterEach
    void cleanup() throws Exception {
        if (tempDir != null) {
            Files.walk(tempDir).sorted((a, b) -> b.getNameCount() - a.getNameCount()).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (Exception ignored) {
                }
            });
        }
    }

    @AfterAll
    static void cleanupAll() throws Exception {
        Path dataDir = java.nio.file.Path.of("data");
        if (!java.nio.file.Files.exists(dataDir))
            return;
        try (java.util.stream.Stream<Path> stream = java.nio.file.Files.list(dataDir)) {
            stream.filter(p -> p.getFileName().toString().startsWith("test-")).forEach(p -> {
                try {
                    java.nio.file.Files.walk(p)
                            .sorted((a, b) -> b.getNameCount() - a.getNameCount()).forEach(q -> {
                                try {
                                    java.nio.file.Files.deleteIfExists(q);
                                } catch (Exception ignored) {
                                }
                            });
                } catch (Exception ignored) {
                }
            });
        }
    }

    @Test
    void processesNosThenCancel() throws Exception {
        tempDir = java.nio.file.Path.of("data", "test-" + System.nanoTime());
        Files.createDirectories(tempDir);
        Path queuePath = tempDir.resolve("fix.q");

        FixMessageBuilder b = new FixMessageBuilder("SND", "TGT");
        String nos1 = b.buildNewOrderSingle("1", "AAPL", '1', 100, 12345, System.nanoTime());
        String nos2 = b.buildNewOrderSingle("2", "MSFT", '2', 200, 23456, System.nanoTime());
        String can1 = b.buildOrderCancelRequest("3", "1", "AAPL", '1', System.nanoTime());

        try (ChronicleQueueService svc = new ChronicleQueueService(queuePath)) {
            svc.writeFix(nos1);
            svc.writeFix(nos2);
            svc.writeFix(can1);
        }

        try (QuickFIXJConsumer consumer = new QuickFIXJConsumer(2)) {
            consumer.consume(queuePath);
            // allow tasks to complete
            Thread.sleep(200);
            var map = consumer.getOpenOrdersMap();
            assertFalse(map.containsKey("1"));
            assertTrue(map.containsKey("2"));
            assertEquals(1, map.size());
        }
    }

    @Test
    void multipleCancelsAndNosInterleaved() throws Exception {
        tempDir = java.nio.file.Path.of("data", "test-" + System.nanoTime());
        Files.createDirectories(tempDir);
        Path queuePath = tempDir.resolve("fix.q");

        FixMessageBuilder b = new FixMessageBuilder("SND", "TGT");
        String nos1 = b.buildNewOrderSingle("1", "AAPL", '1', 100, 12345, System.nanoTime());
        String nos2 = b.buildNewOrderSingle("2", "MSFT", '1', 300, 23456, System.nanoTime());
        String can2 = b.buildOrderCancelRequest("4", "2", "MSFT", '1', System.nanoTime());
        String nos3 = b.buildNewOrderSingle("3", "GOOGL", '2', 200, 34567, System.nanoTime());
        String can1 = b.buildOrderCancelRequest("5", "1", "AAPL", '1', System.nanoTime());

        try (ChronicleQueueService svc = new ChronicleQueueService(queuePath)) {
            // Write interleaved
            svc.writeFix(nos1);
            svc.writeFix(nos2);
            svc.writeFix(can2);
            svc.writeFix(nos3);
            svc.writeFix(can1);
        }

        try (QuickFIXJConsumer consumer = new QuickFIXJConsumer(4)) {
            consumer.consume(queuePath);
            Thread.sleep(300);
            var map = consumer.getOpenOrdersMap();
            assertFalse(map.containsKey("1"));
            assertFalse(map.containsKey("2"));
            assertTrue(map.containsKey("3"));
            assertEquals(1, map.size());
        }
    }
}


