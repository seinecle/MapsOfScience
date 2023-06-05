/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package net.clementlevallois.functions.mapsofscience;

import net.clementlevallois.functions.mapsofscience.queueprocessors.JsonQueueProcessor;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.clementlevallois.utils.Clock;

/**
 *
 * @author LEVALLOIS
 */
public class OperationsOnJson {

    public static void main(String[] args) throws IOException {
        String worksRawData = "data/all-works-sample.json";
        String result = "data/all-works-well-formatted.json";
        OperationsOnJson processingWorks = new OperationsOnJson();
        processingWorks.addClosingCharsToJsonArray(worksRawData, result);
    }

    public void addOpeningCharsToJsonArray(String worksRawData) throws IOException {
        Path path = Path.of(worksRawData);
            try {
            Files.deleteIfExists(path);
            Files.writeString(path, "[", StandardOpenOption.CREATE_NEW,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.APPEND);
            
        } catch (IOException ex) {
            Logger.getLogger(JsonQueueProcessor.class.getName()).log(Level.SEVERE, null, ex);
        }

    }
    

    public void addClosingCharsToJsonArray(String worksRawData, String result) throws IOException {
        Path filePath = Path.of(worksRawData);

        Clock clock = new Clock("removing the final comma");
        try (RandomAccessFile file = new RandomAccessFile(filePath.toString(), "rw")) {
            long length = file.length();
            if (length > 0) {
                // Set the file length to exclude the last character
                file.setLength(length - 1);
                System.out.println("Last character removed from the file.");
            } else {
                System.out.println("The file is empty. No characters to remove.");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        clock.closeAndPrintClock();

        clock = new Clock("adding a final ]");
        char characterToAppend = ']';

        try (RandomAccessFile file = new RandomAccessFile(filePath.toString(), "rw")) {
            long fileLength = file.length();
            file.seek(fileLength);
            file.write(characterToAppend);
            System.out.println("Character appended at the end of the file.");
        } catch (Exception e) {
            e.printStackTrace();
        }
        clock.closeAndPrintClock();
        
        filePath.toFile().renameTo(new File(result));
       
    }

}
