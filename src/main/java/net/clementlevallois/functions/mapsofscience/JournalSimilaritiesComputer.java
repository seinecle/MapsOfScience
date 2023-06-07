/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Main.java to edit this template
 */
package net.clementlevallois.functions.mapsofscience;

import net.clementlevallois.functions.mapsofscience.queueprocessors.StringQueueProcessor;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.objects.ObjectLinkedOpenHashSet;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import net.clementlevallois.utils.Clock;

/**
 *
 * @author LEVALLOIS
 */
public class JournalSimilaritiesComputer {

    static Long2ObjectMap<ObjectLinkedOpenHashSet<Long>> journal2AuthorsMap = new Long2ObjectOpenHashMap();

    static String journalIdsAndAuthorIds = "data/sample-journals-and-authors.txt";
//    static String journalIdsAndAuthorIds = "data/tiny-test.txt";
    static String resultSimilarities = "data/similarities.txt";

    static long maxSize = Long.MAX_VALUE;

    public static void main(String[] args) throws IOException, InterruptedException {
        JournalSimilaritiesComputer computer = new JournalSimilaritiesComputer();
        computer.loadDataToMap(maxSize);
        System.out.println("number of entries in the map: " + journal2AuthorsMap.size());

        Files.deleteIfExists(Path.of(resultSimilarities));
        computer.doubleLoopingThroughJournalIds();
    }

    private void loadDataToMap(long maxSize) throws IOException {
        Clock clock = new Clock("loading file");
        Path inputFilePath = Path.of(journalIdsAndAuthorIds);
        List<String> lines = Files.readAllLines(inputFilePath);

        lines.stream().limit(maxSize).forEach(line -> {
            processLine(line);
        });
        clock.closeAndPrintClock();
    }

    private void doubleLoopingThroughJournalIds() throws IOException, InterruptedException {
        Path result = Path.of(resultSimilarities);
        Clock clock = new Clock("computing similarities");
        ConcurrentLinkedQueue<String> queue = new ConcurrentLinkedQueue();
        int writeToDiskIntervalInSeconds = 5;
        StringQueueProcessor queueProcessor = new StringQueueProcessor(result, queue, writeToDiskIntervalInSeconds);
        Thread queueProcessorThread = new Thread(queueProcessor);
        queueProcessorThread.start();

        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

        LongSet setOfJournalIds = journal2AuthorsMap.keySet();
        long[] arrayOfJournalIds = setOfJournalIds.toLongArray();

        IntStream.range(0, arrayOfJournalIds.length).parallel().forEach(indexJournalA -> {
            long journalIdA = arrayOfJournalIds[indexJournalA];
            Set<Long> authorsA = journal2AuthorsMap.get(arrayOfJournalIds[indexJournalA]);
            IntStream.range(indexJournalA + 1, arrayOfJournalIds.length).parallel().forEach(indexJournalB -> {
                long journalIdB = arrayOfJournalIds[indexJournalB];
                executor.execute(() -> {
                    int similarity = computeSimilarities(authorsA, journal2AuthorsMap.get(journalIdB));
                    if (similarity > 0) {
                        String sim = journalIdA + "," + journalIdB + "," + similarity + "\n";
                        queue.add(sim);
                    }
                });

            });
        });
        executor.shutdown();
        //Awaits either 10 minutes or if all tasks are completed. Whatever is first. I don't know whats a reasonable await time.
        executor.awaitTermination(1L, TimeUnit.MINUTES);

        queueProcessor.stop();
        clock.closeAndPrintClock();
    }

    private int computeSimilarities(Set<Long> authorsOfJournalA, Set<Long> authorsOfJournalB) {
        int counterSimilarity = 0;
        if (authorsOfJournalA.size() < authorsOfJournalB.size()) {
            Iterator<Long> iteratorA = authorsOfJournalA.iterator();
            while (iteratorA.hasNext()) {
                if (authorsOfJournalB.contains(iteratorA.next())) {
                    counterSimilarity++;
                }
            }
        } else {
            Iterator<Long> iteratorB = authorsOfJournalB.iterator();
            while (iteratorB.hasNext()) {
                if (authorsOfJournalA.contains(iteratorB.next())) {
                    counterSimilarity++;
                }
            }
        }
        return counterSimilarity;
    }

    private void processLine(String line) {
        String fields[] = line.split("\\" + Constants.INTER_FIELD_SEPARATOR);
        if (fields.length < 2) {
            return;
        }
        String journalId = fields[0];
        long journalIdAsLong = Long.parseLong(journalId);
        String authorIdsAsLine = fields[1];
        String authorIds[] = authorIdsAsLine.split(Constants.INTRA_FIELD_SEPARATOR);
        ObjectLinkedOpenHashSet<Long> setOfCurrentAuthors = new ObjectLinkedOpenHashSet();
        ObjectLinkedOpenHashSet<Long> setOfAuthorsForThisJournal = journal2AuthorsMap.getOrDefault(journalIdAsLong, setOfCurrentAuthors);
        for (String authorId : authorIds) {
            try {
                long authorIdLong = Long.parseLong(authorId);
                setOfAuthorsForThisJournal.add(authorIdLong);
            } catch (NumberFormatException e) {
                System.out.println("error with author id, not  long: " + authorId);
            }
        }
        journal2AuthorsMap.put(journalIdAsLong, setOfAuthorsForThisJournal);
    }

}
