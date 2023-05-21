/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package net.clementlevallois.functions.mapsofscience.debug;

import java.io.RandomAccessFile;

public class PrintCharactersAroundIndex {
    
    public static void main(String[] args) {
        String filePath = "data/all-works-result.json";
        long index = 10698285128L;
        int n = 5000; // Number of characters before and after the index

        try (RandomAccessFile file = new RandomAccessFile(filePath, "r")) {
            long startIndex = Math.max(index - n, 0);
            long endIndex = Math.min(index + n, file.length() - 1);

            byte[] bytes = new byte[(int) (endIndex - startIndex + 1)];
            file.seek(startIndex);
            file.readFully(bytes);

            String characters = new String(bytes);
            int startIndexInBytes = (int) (index - startIndex);
            int endIndexInBytes = (int) (index - startIndex + 1);

            String before = characters.substring(0, startIndexInBytes);
            String current = characters.substring(startIndexInBytes, endIndexInBytes);
            String after = characters.substring(endIndexInBytes);

            System.out.println("Characters before: " + before);
            System.out.println("Character at index " + index + ": " + current);
            System.out.println("Characters after: " + after);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}