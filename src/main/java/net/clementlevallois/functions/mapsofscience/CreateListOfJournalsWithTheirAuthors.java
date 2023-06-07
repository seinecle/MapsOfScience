/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Main.java to edit this template
 */
package net.clementlevallois.functions.mapsofscience;

import net.clementlevallois.functions.mapsofscience.queueprocessors.StringQueueProcessor;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.objects.ObjectLinkedOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectListIterator;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ConcurrentLinkedQueue;
import net.clementlevallois.utils.Clock;

/**
 *
 * @author LEVALLOIS
 */
public class CreateListOfJournalsWithTheirAuthors {

    /**
     * @param args the command line arguments
     */
    Long2ObjectOpenHashMap<ObjectLinkedOpenHashSet<Long>> journal2AuthorsMap = new Long2ObjectOpenHashMap<>();

    public static void main(String[] args) throws Exception {
        String journalAndAuthorsPerWorkFilePath = "data/journal-and-authors-per-work.txt";
        String outputFileString = "data/all-journals-and-their-authors.txt";
//        String outputFileString = "data/sample-journals-and-authors.txt";
        String journalIdFileString = "data/journal-id-mapping.txt";
        String authorIdFileString = "data/author-id-mapping.txt";
        CreateListOfJournalsWithTheirAuthors create = new CreateListOfJournalsWithTheirAuthors();
//        create.doAllOps(journalAndAuthorsPerWorkFilePath, outputFileString, journalIdFileString, authorIdFileString);
        create.createJournalAndAuthorIdMaps(outputFileString, journalIdFileString, authorIdFileString);
    }

    public void doAllOps(String journalAndAuthorsPerWorkFilePath, String outputFileString, String journalIdFileString, String authorIdFileString) throws Exception {
        loadJournalsAndTheirAuthorsIntoAMap(journalAndAuthorsPerWorkFilePath);
        writeMapToFile(outputFileString);
        mapIdsToSequentialIds(journalIdFileString, authorIdFileString);
    }

    public void createJournalAndAuthorIdMaps(String journalAndAuthorsFilePath, String journalIdFileString, String authorIdFileString) throws Exception {
        loadJournalsAndTheirAuthorsIntoAMap(journalAndAuthorsFilePath);
        mapIdsToSequentialIds(journalIdFileString, authorIdFileString);
    }

    private void loadJournalsAndTheirAuthorsIntoAMap(String filePath) throws Exception {
        Clock clock = new Clock("loading file to map");
        // Open the file using RandomAccessFile and FileChannel
        File file = new File(filePath);
        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
        FileChannel fileChannel = randomAccessFile.getChannel();

        // Create a ByteBuffer to read the file in chunks
        int bufferSize = 8192; // Adjust this according to your needs
        ByteBuffer buffer = ByteBuffer.allocate(bufferSize * 10);

        StringBuilder line = new StringBuilder();
        int count = 0;
        while (fileChannel.read(buffer) != -1) {
            buffer.flip(); // Prepare the buffer for reading

            while (buffer.hasRemaining()) {
                char currentChar = (char) buffer.get();
                if (currentChar == '\n') {
                    // Process the line
                    processLine(line.toString());

                    // Clear the StringBuilder for the next line
                    line.setLength(0);
                } else {
                    line.append(currentChar);
                }
            }
            if (count++ % 3_000 == 0) {
                System.out.print("count buffers: " + count);
                System.out.print(", ");
                clock.printElapsedTime();
// Get the Java runtime
                Runtime runtime = Runtime.getRuntime();
                // Run the garbage collector
                runtime.gc();
                // Calculate the used memory
                long memory = runtime.totalMemory() - runtime.freeMemory();
                System.out.println("Used memory is megabytes: " + (double) memory / (1024 * 1024));
            }

            buffer.clear(); // Prepare the buffer for writing
        }

        // Process the last line if it doesn't end with a newline character
        if (line.length() > 0) {
            processLine(line.toString());
        }

        // Close the file channel and the random access file
        fileChannel.close();
        randomAccessFile.close();
        clock.closeAndPrintClock();
    }

    private void processLine(String line) {
        String fields[] = line.split("\\" + Constants.INTER_FIELD_SEPARATOR);
        if (fields.length < 2) {
            return;
        }
        String journalId = fields[0];
        try {

            long journalIdAsLong = Long.parseLong(journalId);

            String authorIdsAsLine = fields[1];
            String authorIds[] = authorIdsAsLine.split(Constants.INTRA_FIELD_SEPARATOR);
            ObjectLinkedOpenHashSet<Long> setOfCurrentAuthors = new ObjectLinkedOpenHashSet();
            ObjectLinkedOpenHashSet<Long> setOfAuthorsForThisJournal = journal2AuthorsMap.getOrDefault(journalIdAsLong, setOfCurrentAuthors);
            for (String authorId : authorIds) {
                authorId = authorId.trim();
                try {
                    long authorIdLong = Long.parseLong(authorId);
                    setOfAuthorsForThisJournal.add(authorIdLong);
                } catch (NumberFormatException e) {
                    System.out.println("error with author id, not  long: \"" + authorId + "\"");
                }
            }
            journal2AuthorsMap.put(journalIdAsLong, setOfAuthorsForThisJournal);
        } catch (NumberFormatException e) {
            System.out.println("error with journal id, not  long: " + journalId);
        }
    }

    private void writeMapToFile(String outputFileString) throws IOException {
        Path result = Path.of(outputFileString);
        Clock clock = new Clock("writing map to file");
        ConcurrentLinkedQueue<String> queue = new ConcurrentLinkedQueue();
        int writeToDiskIntervalInSeconds = 5;

        StringQueueProcessor queueProcessor = new StringQueueProcessor(result, queue, writeToDiskIntervalInSeconds);
        Thread queueProcessorThread = new Thread(queueProcessor);
        queueProcessorThread.start();

        LongIterator iteratorJournals = journal2AuthorsMap.keySet().iterator();
        StringBuilder sb = new StringBuilder();
        while (iteratorJournals.hasNext()) {
            long journalId = iteratorJournals.nextLong();
            sb.append(String.valueOf(journalId)).append("|");
            ObjectLinkedOpenHashSet<Long> authorIds = journal2AuthorsMap.get(journalId);
            if (authorIds == null || authorIds.isEmpty()) {
                System.out.println("journal without authors: " + journalId);
                continue;
            }
            for (Long authorId : authorIds) {
                sb.append(String.valueOf(authorId)).append(",");
            }
            sb.deleteCharAt(sb.length() - 1);
            sb.append("\n");
            queue.add(sb.toString());
            sb.setLength(0);
        }
        queueProcessor.stop();
        clock.closeAndPrintClock();

    }

    private void mapIdsToSequentialIds(String journalIdFileString, String authorIdFileString) throws IOException {
        // mapping journals;
        Clock clock = new Clock("mapping journal ids and storing them in a file");

        StringBuilder sb = new StringBuilder();
        LongSet journalIds = journal2AuthorsMap.keySet();
        LongIterator journalIdsIterator = journalIds.longIterator();
        int index = 0;
        while (journalIdsIterator.hasNext()) {
            long next = journalIdsIterator.nextLong();
            sb.append(next).append(",").append(index++).append("\n");
        }
        Files.deleteIfExists(Path.of(journalIdFileString));
        Files.writeString(Path.of(journalIdFileString), sb.toString(), StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                StandardOpenOption.APPEND);
        clock.closeAndPrintClock();

        clock = new Clock("mapping author ids and storing them in a file");

        // mapping authors;
        sb = new StringBuilder();
        ObjectLinkedOpenHashSet<Long> authorIds = new ObjectLinkedOpenHashSet();
        LongSet keyset = journal2AuthorsMap.keySet();
        journalIdsIterator = keyset.longIterator();
        index = 0;
        while (journalIdsIterator.hasNext()) {
            ObjectLinkedOpenHashSet<Long> authors = journal2AuthorsMap.get(journalIdsIterator.nextLong());
            authorIds.addAll(authors);
        }
        ObjectListIterator<Long> iteratorAuthors = authorIds.iterator();
        while (iteratorAuthors.hasNext()) {
            Long next = iteratorAuthors.next();
            sb.append(next).append(",").append(index++).append("\n");
        }
        Files.deleteIfExists(Path.of(authorIdFileString));
        Files.writeString(Path.of(authorIdFileString), sb.toString(), StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                StandardOpenOption.APPEND);
        clock.closeAndPrintClock();

    }

}
