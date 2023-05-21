/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package net.clementlevallois.functions.mapsofscience;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import net.clementlevallois.utils.Clock;

/**
 *
 * @author LEVALLOIS
 */
public class ProcessingWorks {

    private final String worksRawData = "data/all-works-partial.json";
    private final String result = "data/all-works-result.json";

    public static void main(String[] args) throws IOException {
        ProcessingWorks processingWorks = new ProcessingWorks();
        processingWorks.formatAsJsonArray();
    }

    private void formatAsJsonArray() throws IOException {
        Path filePath = Path.of(worksRawData);
        Path resultPath = Path.of(result);
        Path tempFilePath = Path.of("data/temp.tmp");
        
        Files.deleteIfExists(resultPath);
        Files.deleteIfExists(tempFilePath);
        Files.createFile(tempFilePath);
        
        Clock clock = new Clock("general");
        
        // create a buffered input stream and buffered output stream with a small buffer size
        try (BufferedInputStream in = new BufferedInputStream(Files.newInputStream(filePath, StandardOpenOption.READ));
                BufferedOutputStream out = new BufferedOutputStream(Files.newOutputStream(tempFilePath, StandardOpenOption.WRITE))) {

            // write the '[' character at the beginning of the output file
            out.write('[');

            // copy the contents of the input file to the output file, excluding the last character
            int bufferSize = 8192;
            byte[] buffer = new byte[bufferSize * 2];
            int bytesRead;
//            byte lastByte = 0;
            while ((bytesRead = in.read(buffer, 0, bufferSize)) != -1) {
                if (bytesRead < bufferSize) {
                    // save the last byte read from the input file
//                    lastByte = buffer[bytesRead - 1];
                    bytesRead--;
                }
                out.write(buffer, 0, bytesRead);
            }

            // write the ']' character at the end of the output file
            out.write(']');
        }

        // delete the input file and rename the output file to the input file name
//        Files.delete(filePath);
        Files.move(tempFilePath, resultPath, StandardCopyOption.REPLACE_EXISTING);
        clock.closeAndPrintClock();
        
    }

}
