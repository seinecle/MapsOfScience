/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package net.clementlevallois.functions.mapsofscience;

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

    private static final int MAX_BYTES_PER_WRITE = 1024 * 1024; // 1 MB
    private static  Duration FLUSH_INTERVAL = Duration.ofSeconds(30);

    private final Path outputFilePath;
    private final ConcurrentLinkedQueue<String> jsonQueue;
    private boolean apiCallsRunning = true;

    public JsonQueueProcessor(Path outputFilePath, ConcurrentLinkedQueue<String> jsonQueue, int flushIntervalInSeconds) {
        this.outputFilePath = outputFilePath;
        this.jsonQueue = jsonQueue;
        FLUSH_INTERVAL = Duration.ofSeconds(flushIntervalInSeconds);
    }

    public void stop() {
        apiCallsRunning = false;
    }

    @Override
    public void run() {
        Instant lastFlushTime = Instant.now();
        try (FileChannel outputChannel = FileChannel.open(outputFilePath,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.APPEND)) {
            while (apiCallsRunning) {
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
        } catch (IOException | InterruptedException e) {
            // Handle exceptions as appropriate for your application
        }
    }
}
