/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package net.clementlevallois.functions.mapsofscience;

import jakarta.json.Json;
import jakarta.json.stream.JsonParser;
import jakarta.json.stream.JsonParser.Event;
import jakarta.json.stream.JsonParsingException;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedQueue;
import net.clementlevallois.utils.Clock;

/**
 *
 * @author LEVALLOIS
 */
public class JsonStreamingParser {

    public static void main(String args[]) throws FileNotFoundException, IOException {
        String pathString = "data/all-works-well-formatted.json";
        String pathResultString = "data/journal-and-authors-per-work.txt";
        JsonStreamingParser parser = new JsonStreamingParser();
        parser.parseJournalIdsAndAuthorIds(pathString, pathResultString);
    }

    public void parseJournalIdsAndAuthorIds(String pathString, String pathResultString) throws FileNotFoundException, IOException {
        Path path = Path.of(pathString);
        Path result = Path.of(pathResultString);
        Clock clock = new Clock("general clock");
        ConcurrentLinkedQueue<String> queue = new ConcurrentLinkedQueue();
        int writeToDiskIntervalInSeconds = 5;
        StringQueueProcessor queueProcessor = new StringQueueProcessor(result, queue, writeToDiskIntervalInSeconds);
        Thread queueProcessorThread = new Thread(queueProcessor);
        queueProcessorThread.start();
        InputStreamReader isr = new InputStreamReader(new FileInputStream(path.toFile()), StandardCharsets.UTF_8);
        BufferedReader buffReader = new BufferedReader(isr);
        JsonParser jsonParser = Json.createParser(buffReader);

        boolean authorStarted = false;
        boolean primaryLocationStarted = false;
        boolean sourceStarted = false;
        StringBuilder sbTemp = new StringBuilder();
        int count = 0;
        try {
            while (jsonParser.hasNext()) {
                Event e;
                try {
                    e = jsonParser.next();
                } catch (Exception ex) {
                    continue;
                }

                if (e == Event.KEY_NAME) {
                    String keyName = jsonParser.getString();
                    if (keyName.equals("authorships")) {
                        sbTemp = new StringBuilder();
                    }
                    if (keyName.equals("author")) {
                        authorStarted = true;
                    }
                    if (keyName.equals("primary_location")) {
                        primaryLocationStarted = true;
                    }
                    if (primaryLocationStarted && keyName.equals("source")) {
                        sourceStarted = true;
                    }
                    if (sourceStarted && keyName.equals("id")) {
                        jsonParser.next();
                        if (jsonParser.currentEvent() != Event.VALUE_STRING) {
                            continue;
                        }
                        String valueOfJournalId = jsonParser.getString();
                        String lastPartOfJournalId = keepLastPartOfId(valueOfJournalId);
                        sbTemp.insert(0, "|");
                        sbTemp.insert(0, lastPartOfJournalId);
                        sbTemp.deleteCharAt(sbTemp.length() - 1);
                        sbTemp.append("\n");
                        queue.add(sbTemp.toString());
                        count++;
                        sourceStarted = false;
                        primaryLocationStarted = false;
                        if (count % 100_000 == 0) {
                            System.out.print("count: " + count);
                            System.out.print(", ");
                            clock.printElapsedTime();
                        }
                    }
                    if (authorStarted && keyName.equals("id")) {
                        jsonParser.next();
                        if (jsonParser.currentEvent() != Event.VALUE_STRING) {
                            continue;
                        }
                        String valueOfAuthorId = jsonParser.getString();
                        String lastPartOfAuthorId = keepLastPartOfId(valueOfAuthorId);
                        sbTemp.append(lastPartOfAuthorId);
                        sbTemp.append(",");
                        authorStarted = false;
                    }
                }
            }
        } catch (JsonParsingException exception) {
            System.out.println("location: " + jsonParser.getLocation().getStreamOffset());
            System.out.println("current valid line: " + sbTemp.toString());
        }
        queueProcessor.stop();
        queueProcessorThread.interrupt();
        clock.closeAndPrintClock();
    }

    private String keepLastPartOfId(String fullId) {
        String[] idFields = fullId.split("/");
        String idWithLetter = idFields[idFields.length - 1];
        return idWithLetter.substring(1);
    }

}
