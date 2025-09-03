package com.fix.performance.queue;

import java.io.Closeable;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;

/**
 * Minimal Chronicle Queue wrapper for writing/reading FIX messages. Messages are stored as
 * documents with a single text field "fix".
 */
public final class ChronicleQueueService implements Closeable {
    private final ChronicleQueue queue;
    private final ExcerptAppender appender;

    public ChronicleQueueService(Path path) {
        this.queue = SingleChronicleQueueBuilder.binary(path.toString()).build();
        // Use createAppender() for compatibility across Chronicle Queue versions
        this.appender = this.queue.createAppender();
    }

    public void writeFix(String fixMessage) {
        appender.writeDocument(w -> w.write("fix").text(fixMessage));
    }

    public List<String> readAll() {
        List<String> messages = new ArrayList<>();
        ExcerptTailer tailer = queue.createTailer();
        while (true) {
            try (DocumentContext dc = tailer.readingDocument()) {
                if (!dc.isPresent())
                    break;
                String msg = dc.wire().read("fix").text();
                messages.add(msg);
            }
        }
        return messages;
    }

    public void forEach(Consumer<String> consumer) {
        ExcerptTailer tailer = queue.createTailer();
        while (true) {
            try (DocumentContext dc = tailer.readingDocument()) {
                if (!dc.isPresent())
                    break;
                String msg = dc.wire().read("fix").text();
                consumer.accept(msg);
            }
        }
    }

    @Override
    public void close() {
        queue.close();
    }
}


