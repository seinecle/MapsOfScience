/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package net.clementlevallois.functions.mapsofscience;

import jakarta.json.Json;
import jakarta.json.stream.JsonParser;
import jakarta.json.stream.JsonParser.Event;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 *
 * @author LEVALLOIS
 */
public class JsonStreamingParser {

    public static void main(String args[]) throws FileNotFoundException, IOException {
        JsonStreamingParser parser = new JsonStreamingParser();
        Path path = Path.of("data/all-works-result.json");
        Path result = Path.of("data/journal-and-authors-per-work.txt");
        parser.parseJournalIdsAndAuthorIds(path, result);
    }

    public void parseJournalIdsAndAuthorIds(Path path, Path pathResult) throws FileNotFoundException, IOException {

        JsonParser jsonParser = Json.createParser(new FileInputStream(path.toFile()));

        boolean authorStarted = false;
        boolean primaryLocationStarted = false;
        boolean sourceStarted = false;
        StringBuilder sbTotal = new StringBuilder();
        StringBuilder sbTemp = new StringBuilder();
        while (jsonParser.hasNext()) {
            Event e = jsonParser.next();

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
                    String valueOfJournalId = jsonParser.getString();
                    String lastPartOfJournalId = keepLastPartOfId(valueOfJournalId);
                    sbTemp.insert(0,"|");
                    sbTemp.insert(0,lastPartOfJournalId);
                    sbTemp.deleteCharAt(sbTemp.length() - 1);
                    sbTemp.append("\n");
                    sbTotal.append(sbTemp);
                    sourceStarted = false;
                    primaryLocationStarted = false;
                }
                if (authorStarted && keyName.equals("id")) {
                    jsonParser.next();
                    String valueOfAuthorId = jsonParser.getString();
                    String lastPartOfAuthorId = keepLastPartOfId(valueOfAuthorId);
                    sbTemp.append(lastPartOfAuthorId);
                    sbTemp.append(",");
                    authorStarted = false;
                }
            }
        }
        Files.writeString(pathResult, sbTotal.toString());
    }

    private String keepLastPartOfId(String fullId) {
        String[] idFields = fullId.split("/");
        return idFields[idFields.length - 1];
    }

}
