/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package net.clementlevallois.functions.mapsofscience.debug;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class FindStringInLargeFile {
    public static void main(String[] args) {
        String filePath = "data/all-works-result.json";
        String searchString = "https://doi.org/10.1016/j.psep.2018.06.013";
        int n = 1000; // Number of characters before and after the found string

        try (RandomAccessFile file = new RandomAccessFile(filePath, "r")) {
            FileChannel channel = file.getChannel();
            ByteBuffer buffer = ByteBuffer.allocate(8192);
            long position = 0;
            long fileSize = channel.size();

            // Read the file in chunks until the end
            while (position < fileSize) {
                buffer.clear();
                channel.read(buffer, position);
                buffer.flip();

                // Convert buffer content to a string
                String chunk = new String(buffer.array(), 0, buffer.limit());

                // Search for the target string in the chunk
                int foundIndex = chunk.indexOf(searchString);

                if (foundIndex != -1) {
                    // Calculate the start and end positions
                    long start = position + foundIndex - n;
                    long end = position + foundIndex + searchString.length() + n;

                    // Adjust the start and end positions if they exceed file boundaries
                    start = Math.max(start, 0);
                    end = Math.min(end, fileSize - 1);

                    // Read and print the characters before and after the found string
                    printCharactersInRange(channel, start, end);

                    break;
                }

                position += buffer.limit();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void printCharactersInRange(FileChannel channel, long start, long end) throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate((int) (end - start + 1));
        channel.read(buffer, start);
        buffer.flip();

        String characters = new String(buffer.array(), 0, buffer.limit());
        System.out.println("Characters before and after the found string: " + characters);
    }
}
