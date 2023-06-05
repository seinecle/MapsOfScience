/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package net.clementlevallois.functions.mapsofscience.queueprocessors;

/**
 *
 * @author ChatGPT and LEVALLOIS
 */
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ConcurrentLinkedQueue;

public class JsonQueueProcessor implements Runnable {

    private static final int MAX_BYTES_PER_WRITE = 1024 * 1024 * 20; // 20 MB
    private static Duration FLUSH_INTERVAL = Duration.ofMillis(500);

    private boolean operationsRunning;

    private final Path outputFilePath;
    private final ConcurrentLinkedQueue<String> jsonQueue;

    public JsonQueueProcessor(Path outputFilePath, ConcurrentLinkedQueue<String> jsonQueue, int flushIntervalInSeconds) {
        this.outputFilePath = outputFilePath;
        this.jsonQueue = jsonQueue;
        FLUSH_INTERVAL = Duration.ofSeconds(flushIntervalInSeconds);
        operationsRunning = true;
    }

    public void stop() {
        operationsRunning = false;
    }

    @Override
    public void run() {
        Instant lastFlushTime = Instant.now();
        try (FileChannel outputChannel = FileChannel.open(outputFilePath,
                StandardOpenOption.WRITE,
                StandardOpenOption.APPEND)) {
            while (operationsRunning || !jsonQueue.isEmpty()) {
                String json = jsonQueue.poll();
                if (json == null) {
                    // No more items in the queue, sleep for a bit before checking again
                    Thread.sleep(100);
                    continue;
                }
                json = json + ",";

                ByteBuffer byteBuffer = ByteBuffer.wrap(json.getBytes(StandardCharsets.UTF_8));
                do {
                    int bytesToWrite = Math.min(byteBuffer.remaining(), MAX_BYTES_PER_WRITE);
                    byteBuffer.limit(byteBuffer.position() + bytesToWrite);
                    outputChannel.write(byteBuffer);
                    byteBuffer.compact();
                } while (byteBuffer.position() > 0);

                Instant now = Instant.now();
                if (Duration.between(lastFlushTime, now).compareTo(FLUSH_INTERVAL) >= 0) {
                    outputChannel.force(true);
                    lastFlushTime = now;
                }
            }
            Thread.currentThread().interrupt();

        } catch (IOException | InterruptedException e) {
            System.out.println("io exception while writing json to disk");
        }
    }
}
