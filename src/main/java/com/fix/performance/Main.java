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
                    logger.error("Usage: flyweight <queue_path> [enable_metrics]");
                    return;
                }
                boolean enableMetrics = args.length > 2 ? Boolean.parseBoolean(args[2]) : true;
                runFlyweightConsumer(args[1], enableMetrics);
            }
            case "quickfixj" -> {
                if (args.length < 3) {
                    logger.error("Usage: quickfixj <queue_path> <thread_count>");
                    return;
                }
                runQuickFIXJConsumer(args[1], Integer.parseInt(args[2]));
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
        System.out.println("  flyweight <queue_path> [enable_metrics] - Run flyweight consumer");
        System.out.println("  quickfixj <queue_path> <thread_count>  - Run QuickFIX/J consumer");
        System.out.println();
        System.out.println("Examples:");
        System.out.println("  java -jar fix-performance-test.jar generate ./data/fix.q 2000000");
        System.out.println("  java -jar fix-performance-test.jar flyweight ./data/fix.q true");
        System.out.println("  java -jar fix-performance-test.jar quickfixj ./data/fix.q 8");
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

    private static void runFlyweightConsumer(String queuePath, boolean enableMetrics) {
        logger.info("Starting flyweight consumer: queue={}, metrics={}", queuePath, enableMetrics);
        java.nio.file.Path path = java.nio.file.Path.of(queuePath);
        try (com.fix.performance.FlyweightConsumer consumer =
                new com.fix.performance.FlyweightConsumer()) {
            consumer.consume(path);
        }
    }

    private static void runQuickFIXJConsumer(String queuePath, int threadCount) {
        logger.info("Starting QuickFIX/J consumer: queue={}, threads={}", queuePath, threadCount);
        java.nio.file.Path path = java.nio.file.Path.of(queuePath);
        try (com.fix.performance.QuickFIXJConsumer consumer =
                new com.fix.performance.QuickFIXJConsumer(threadCount)) {
            consumer.consume(path);
        }
    }
}
