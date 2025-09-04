package com.fix.performance;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Main entry point for FIX performance testing application.
 * 
 * This application compares two FIX.4.4 message processing approaches: 1. Flyweight/off-heap
 * processing on a single pinned thread 2. QuickFIX/J parsing with thread pool
 */
public class Main {
    private static final Logger logger = LogManager.getLogger(Main.class);

    public static void main(String[] args) {
        logger.info("FIX Performance Test Application Starting...");

        if (args.length == 0) {
            printUsage();
            return;
        }

        String command = args[0].toLowerCase();

        switch (command) {
            case "generate" -> {
                if (args.length < 3) {
                    logger.error("Usage: generate <queue_path> <message_count>");
                    return;
                }
                runGenerator(args[1], Long.parseLong(args[2]));
            }
            case "flyweight" -> {
                if (args.length < 2) {
                    logger.error("Usage: flyweight <queue_path> [metrics_path]");
                    return;
                }
                String metricsPath = args.length > 2 ? args[2] : null;
                runFlyweightConsumer(args[1], metricsPath);
            }
            case "quickfixj" -> {
                if (args.length < 3) {
                    logger.error("Usage: quickfixj <queue_path> <thread_count> [metrics_path]");
                    return;
                }
                String metricsPath = args.length > 3 ? args[3] : null;
                runQuickFIXJConsumer(args[1], Integer.parseInt(args[2]), metricsPath);
            }
            default -> {
                logger.error("Unknown command: {}", command);
                printUsage();
            }
        }
    }

    private static void printUsage() {
        System.out.println("FIX Performance Test Application");
        System.out.println("Usage: java -jar fix-performance-test.jar <command> [args...]");
        System.out.println();
        System.out.println("Commands:");
        System.out.println("  generate <queue_path> <message_count>  - Generate FIX messages");
        System.out.println("  flyweight <queue_path> [metrics_path]  - Run flyweight consumer");
        System.out.println(
                "  quickfixj <queue_path> <thread_count> [metrics_path]  - Run QuickFIX/J consumer");
        System.out.println();
        System.out.println("Examples:");
        System.out.println("  java -jar fix-performance-test.jar generate ./data/fix.q 2000000");
        System.out.println(
                "  java -jar fix-performance-test.jar flyweight ./data/fix.q ./metrics/fly.txt");
        System.out.println(
                "  java -jar fix-performance-test.jar quickfixj ./data/fix.q 8 ./metrics/qfj.txt");
    }

    private static void runGenerator(String queuePath, long messageCount) {
        logger.info("Starting FIX message generator: queue={}, count={}", queuePath, messageCount);
        com.fix.performance.generator.FixMessageGenerator gen =
                new com.fix.performance.generator.FixMessageGenerator();
        java.nio.file.Path path = java.nio.file.Path.of(queuePath);
        com.fix.performance.generator.GenerationResult res =
                gen.generate(path, messageCount, System.nanoTime(), "SENDER", "TARGET");
        logger.info("Generation done: total={}, nos={}, cancels={}", res.totalMessages(),
                res.nosCount(), res.cancelCount());
    }

    private static void runFlyweightConsumer(String queuePath, String metricsPath) {
        logger.info("Starting flyweight consumer: queue={}, metricsPath={}", queuePath,
                metricsPath);
        java.nio.file.Path path = java.nio.file.Path.of(queuePath);
        try (com.fix.performance.FlyweightConsumer consumer =
                new com.fix.performance.FlyweightConsumer()) {
            java.nio.file.Path m =
                    java.nio.file.Path.of(metricsPath != null ? metricsPath : "./metrics/fly.txt");
            consumer.consume(path, m);
        }
    }

    private static void runQuickFIXJConsumer(String queuePath, int threadCount,
            String metricsPath) {
        logger.info("Starting QuickFIX/J consumer: queue={}, threads={}, metricsPath={}", queuePath,
                threadCount, metricsPath);
        java.nio.file.Path path = java.nio.file.Path.of(queuePath);
        try (com.fix.performance.QuickFIXJConsumer consumer =
                new com.fix.performance.QuickFIXJConsumer(threadCount)) {
            if (metricsPath == null)
                consumer.consume(path);
            else
                consumer.consume(path, java.nio.file.Path.of(metricsPath));
        }
    }
}
