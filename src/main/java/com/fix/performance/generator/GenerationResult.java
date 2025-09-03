package com.fix.performance.generator;

/** Result of FIX message generation. */
public record GenerationResult(long totalMessages, long nosCount, long cancelCount,
        long firstPhaseMessages) {
}


