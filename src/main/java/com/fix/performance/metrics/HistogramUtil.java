package com.fix.performance.metrics;

import java.nio.file.Files;
import java.nio.file.Path;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;

public final class HistogramUtil {
    private HistogramUtil() {}

    public static void writeHistogram(Path out, Recorder recorder, String label) {
        try {
            if (out == null)
                return;
            if (out.getParent() != null)
                Files.createDirectories(out.getParent());
            Histogram h = recorder.getIntervalHistogram();
            long count = h.getTotalCount();
            String content = label + " latency (ns)\n" + "count=" + count + "\n" + "p50="
                    + h.getValueAtPercentile(50) + "\n" + "p90=" + h.getValueAtPercentile(90) + "\n"
                    + "p99=" + h.getValueAtPercentile(99) + "\n" + "p99.9="
                    + h.getValueAtPercentile(99.9) + "\n" + "max=" + h.getMaxValue() + "\n"
                    + "mean=" + (long) h.getMean() + "\n";
            Files.writeString(out, content);
        } catch (Exception ignored) {
        }
    }
}


