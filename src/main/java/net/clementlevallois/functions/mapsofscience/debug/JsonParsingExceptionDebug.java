package net.clementlevallois.functions.mapsofscience.debug;

import jakarta.json.Json;
import jakarta.json.stream.JsonParser;

import java.io.FileReader;
import java.io.RandomAccessFile;

public class JsonParsingExceptionDebug {

    public static void main(String[] args) {
        String filePath = "data/all-works-result.json";
        int columnNo = 2000;
        long offset = 10698284928l;

        try (JsonParser parser = Json.createParser(new FileReader(filePath))) {
            // Skip to the position of the exception
            skipToPosition(parser, offset);

            // Read a portion of the file corresponding to the exception
            String portion = readPortionOfFile(filePath, offset, columnNo);

            System.out.println("Portion of the file:");
            System.out.println(portion);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void skipToPosition(JsonParser parser, long offset) {
        while (parser.getLocation().getStreamOffset() < offset) {
            parser.next();
        }
    }

    private static String readPortionOfFile(String filePath, long offset, int length) throws Exception {
        try (RandomAccessFile file = new RandomAccessFile(filePath, "r")) {
            file.seek(offset);
            byte[] bytes = new byte[length];
            int bytesRead = file.read(bytes);
            return new String(bytes, 0, bytesRead);
        }
    }
}
