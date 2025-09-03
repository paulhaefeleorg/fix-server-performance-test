package com.fix.performance;

import java.nio.file.Path;
import com.fix.performance.generator.FixMessageGenerator;
import com.fix.performance.generator.GenerationResult;

/**
 * Simple CLI entry to run the generator directly, keeping backward compatibility with
 * build.gradle's runGenerate task.
 */
public final class Generator {
    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: Generator <queue_path> <message_count>");
            System.exit(2);
            return;
        }
        Path queue = Path.of(args[0]);
        long count = Long.parseLong(args[1]);

        FixMessageGenerator gen = new FixMessageGenerator();
        GenerationResult res = gen.generate(queue, count, System.nanoTime(), "SENDER", "TARGET");
        System.out.printf("Generated: total=%d, NOS=%d, Cancel=%d%n", res.totalMessages(),
                res.nosCount(), res.cancelCount());
    }
}


