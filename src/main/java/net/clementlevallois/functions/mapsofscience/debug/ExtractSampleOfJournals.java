/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Main.java to edit this template
 */
package net.clementlevallois.functions.mapsofscience.debug;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.stream.Stream;

/**
 *
 * @author LEVALLOIS
 */
public class ExtractSampleOfJournals {

    public static void copyFirstNLines(String sourceFilePath, String destinationFilePath, int n) throws IOException {
        try (Stream<String> lines = Files.lines(Path.of(sourceFilePath), StandardCharsets.UTF_8)) {
            Path tempFilePath = Files.createTempFile("temp", ".txt");
            try (FileChannel sourceChannel = FileChannel.open(Path.of(sourceFilePath), StandardOpenOption.READ); FileChannel tempChannel = FileChannel.open(tempFilePath, StandardOpenOption.WRITE)) {
                FileLock lock = sourceChannel.lock(0, Long.MAX_VALUE, true);  // Acquire an exclusive lock on the source file

                // Copy the first N lines to the temporary file
                lines.limit(n).forEach(line -> {
                    byte[] lineBytes = (line + System.lineSeparator()).getBytes(StandardCharsets.UTF_8);
                    ByteBuffer buffer = ByteBuffer.wrap(lineBytes);
                    try {
                        tempChannel.write(buffer);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });

                lock.release();  // Release the lock on the source file
            }

            // Move the temporary file to the destination file
            Files.move(tempFilePath, Path.of(destinationFilePath), StandardCopyOption.REPLACE_EXISTING);
        }
    }

    public static void main(String[] args) {
        try {
            copyFirstNLines("data/all-journals-and-their-authors.txt", "data/sample-journals-and-authors.txt", 5_000);  // Example usage
            System.out.println("First N lines copied successfully.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
