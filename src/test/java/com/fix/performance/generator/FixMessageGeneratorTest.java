package com.fix.performance.generator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import com.fix.performance.queue.ChronicleQueueService;

public class FixMessageGeneratorTest {
    private Path tempDir;

    @AfterEach
    void cleanup() throws Exception {
        if (tempDir != null) {
            // Best-effort recursive delete
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
    void generatesRequestedMessagesAndFlushesCancels() throws Exception {
        tempDir = java.nio.file.Path.of("data", "test-" + System.nanoTime());
        Files.createDirectories(tempDir);
        Path queuePath = tempDir.resolve("fix.q");

        FixMessageGenerator gen = new FixMessageGenerator();
        long requested = 500;
        GenerationResult res = gen.generate(queuePath, requested, 42L, "SND", "TGT");

        assertTrue(res.totalMessages() >= requested,
                "Total messages should be at least requested (due to flush)");
        assertEquals(res.nosCount() + res.cancelCount(), res.totalMessages());

        try (ChronicleQueueService svc = new ChronicleQueueService(queuePath)) {
            List<String> all = svc.readAll();
            assertEquals(res.totalMessages(), all.size(),
                    "Queue should contain all generated messages");

            long nos = all.stream().filter(m -> m.contains("35=D\u0001")).count();
            long cancels = all.stream().filter(m -> m.contains("35=F\u0001")).count();
            assertEquals(res.nosCount(), nos, "NOS count should match");
            assertEquals(res.cancelCount(), cancels, "Cancel count should match");

            // Validate required fields presence
            assertTrue(all.stream().allMatch(m -> m.startsWith("8=FIX.4.4\u00019=")),
                    "All have BeginString and BodyLength");
            assertTrue(all.stream().allMatch(m -> m.contains("\u000110=")), "All have CheckSum");
        }
    }

    @Test
    void schedulesCancelsWithin100Messages() throws Exception {
        tempDir = java.nio.file.Path.of("data", "test-" + System.nanoTime());
        Files.createDirectories(tempDir);
        Path queuePath = tempDir.resolve("fix.q");

        FixMessageGenerator gen = new FixMessageGenerator();
        long requested = 200;
        GenerationResult res = gen.generate(queuePath, requested, 1337L, "SND", "TGT");

        try (ChronicleQueueService svc = new ChronicleQueueService(queuePath)) {
            List<String> all = svc.readAll();

            // Build map of first occurrence positions for each ClOrdID and its cancel
            java.util.Map<String, Integer> nosIndex = new java.util.HashMap<>();
            java.util.Map<String, Integer> cancelIndex = new java.util.HashMap<>();

            for (int i = 0; i < all.size(); i++) {
                String m = all.get(i);
                if (m.contains("35=D\u0001")) {
                    String cl = tag(m, "11");
                    nosIndex.putIfAbsent(cl, i);
                } else if (m.contains("35=F\u0001")) {
                    String orig = tag(m, "41");
                    cancelIndex.putIfAbsent(orig, i);
                }
            }

            // Verify each NOS got a cancel and the cancel distance is <= 100 in the first phase, or
            // after flush
            for (var e : nosIndex.entrySet()) {
                String cl = e.getKey();
                assertTrue(cancelIndex.containsKey(cl), "Every NOS should have a cancel");
                int dist = cancelIndex.get(cl) - e.getValue();
                assertTrue(dist > 0, "Cancel must come after NOS");
                // Dist can exceed 100 only if it was flushed after budget, so allow up to total
                // size
                assertTrue(dist <= 100 || e.getValue() >= res.firstPhaseMessages(),
                        "Cancel should be within 100 unless flushed at the end");
            }
        }
    }

    private static String tag(String msg, String tag) {
        String needle = tag + "=";
        int i = msg.indexOf(needle);
        if (i < 0)
            return "";
        int s = i + needle.length();
        int e = msg.indexOf('\u0001', s);
        if (e < 0)
            e = msg.length();
        return msg.substring(s, e);
    }

    @Test
    void pricesPerSymbolStayWithinTenCentsAndOnlyFiveSymbolsUsed() throws Exception {
        tempDir = java.nio.file.Path.of("data", "test-" + System.nanoTime());
        Files.createDirectories(tempDir);
        Path queuePath = tempDir.resolve("fix.q");

        FixMessageGenerator gen = new FixMessageGenerator();
        long requested = 1000;
        gen.generate(queuePath, requested, 7L, "SND", "TGT");

        try (ChronicleQueueService svc = new ChronicleQueueService(queuePath)) {
            List<String> all = svc.readAll();

            java.util.Map<String, java.util.List<Long>> symbolToCents = new java.util.HashMap<>();
            java.util.Set<String> seenSymbols = new java.util.HashSet<>();

            for (String m : all) {
                if (m.contains("35=D\u0001")) {
                    String symbol = tag(m, "55");
                    String priceStr = tag(m, "44");
                    if (!symbol.isEmpty() && !priceStr.isEmpty()) {
                        seenSymbols.add(symbol);
                        long cents = toCents(priceStr);
                        symbolToCents.computeIfAbsent(symbol, k -> new java.util.ArrayList<>())
                                .add(cents);
                    }
                }
            }

            java.util.Set<String> expected =
                    java.util.Set.of("AAPL", "MSFT", "GOOGL", "AMZN", "META");
            assertTrue(seenSymbols.stream().allMatch(expected::contains),
                    "Only expected symbols should appear");
            assertTrue(seenSymbols.size() <= 5, "No more than five symbols should be used");

            for (var e : symbolToCents.entrySet()) {
                java.util.List<Long> cents = e.getValue();
                if (cents.size() <= 1)
                    continue; // cannot evaluate range with 0/1 datapoint
                long min = cents.stream().mapToLong(Long::longValue).min().orElseThrow();
                long max = cents.stream().mapToLong(Long::longValue).max().orElseThrow();
                assertTrue(max - min <= 20,
                        "Prices for symbol " + e.getKey() + " exceed +/-10 cents window");
            }
        }
    }

    private static long toCents(String price) {
        // Avoid floating errors by parsing manually
        int dot = price.indexOf('.');
        if (dot < 0) {
            return Long.parseLong(price) * 100L;
        }
        String dollars = price.substring(0, dot);
        String frac = price.substring(dot + 1);
        if (frac.length() == 1)
            frac = frac + "0";
        if (frac.length() > 2)
            frac = frac.substring(0, 2);
        return Long.parseLong(dollars) * 100L + Long.parseLong(frac);
    }
}


