/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Main.java to edit this template
 */
package net.clementlevallois.functions.mapsofscience;

import it.unimi.dsi.fastutil.doubles.DoubleOpenHashSet;
import it.unimi.dsi.fastutil.doubles.DoubleSet;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.ints.IntSets;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.longs.LongSets;
import it.unimi.dsi.fastutil.objects.ReferenceOpenHashSet;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import net.clementlevallois.utils.Clock;

/**
 *
 * @author LEVALLOIS
 */
public class JournalSimilaritiesComputer {

    static Long2ObjectOpenHashMap<ReferenceOpenHashSet<Long>> journal2AuthorsMap = new Long2ObjectOpenHashMap<>();

    String journalIdsAndAuthorIds = "data/small-sample-journals-and-authors.txt";
    String resultSimilarities = "data/similarities.txt";

    public static void main(String[] args) throws IOException {
        JournalSimilaritiesComputer computer = new JournalSimilaritiesComputer();
        computer.loadDataToMap();
        System.out.println("number of entries in the map: " + journal2AuthorsMap.size());
        computer.doubleLoopingThroughJournalIds();
    }

    private void loadDataToMap() throws IOException {
        Path inputFilePath = Path.of(journalIdsAndAuthorIds);
        List<String> lines = Files.readAllLines(inputFilePath);

        lines.stream().forEach(line -> {
            processLine(line);
        });
    }

    private void doubleLoopingThroughJournalIds() {
        Path result = Path.of(resultSimilarities);
        Clock clock = new Clock("computing similarities");
        ConcurrentLinkedQueue<String> queue = new ConcurrentLinkedQueue();
        int writeToDiskIntervalInSeconds = 5;
        StringQueueProcessor queueProcessor = new StringQueueProcessor(result, queue, writeToDiskIntervalInSeconds);
        Thread queueProcessorThread = new Thread(queueProcessor);
        queueProcessorThread.start();

        IntOpenHashSet pairsAlreadyChecked = new IntOpenHashSet();
        IntSet synchronizedSet = IntSets.synchronize(pairsAlreadyChecked);
        
        journal2AuthorsMap.keySet().longParallelStream().forEach(journalIdA -> {
            journal2AuthorsMap.keySet().longParallelStream().forEach(journalIdB -> {
                if (journalIdA == journalIdB) {
                    return;
                }
                String jA = String.valueOf(journalIdA);
                String jB = String.valueOf(journalIdB);
                
                if (journalIdA < journalIdB) {
                    int hash = (jA + jB).hashCode();
                    if (synchronizedSet.contains(hash)){
                        return;
                    }
                    synchronizedSet.add(hash);
                } else {
                    int hash = (jB + jA).hashCode();
                    if (synchronizedSet.contains(hash)){
                        return;
                    }
                    synchronizedSet.add(hash);
                }
                Integer similarity = computeSimilarities(journalIdA, journalIdB);
                if (similarity > 0) {
                    String sim = journalIdA + "," + journalIdB + "," + similarity;
                    queue.add(sim);
                }
            });
        });
        queueProcessor.stop();
        queueProcessorThread.interrupt();
        clock.closeAndPrintClock();
    }

    private Integer computeSimilarities(long journalIdA, long journalIdB) {
        ReferenceOpenHashSet<Long> authorsOfJournalA = journal2AuthorsMap.get(journalIdA);
        ReferenceOpenHashSet<Long> authorsOfJournalB = journal2AuthorsMap.get(journalIdB);
        
        for (Long entry: authorsOfJournalA){
            if (authorsOfJournalB.contains(entry)){
                System.out.println("stop");
            }
        }
        Set<Long> commonElements = authorsOfJournalA.parallelStream()
                .filter(authorsOfJournalB::contains)
                .collect(Collectors.toSet());
        return commonElements.size();
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
        ReferenceOpenHashSet<Long> setOfCurrentAuthors = new ReferenceOpenHashSet();
        ReferenceOpenHashSet<Long> setOfAuthorsForThisJournal = journal2AuthorsMap.getOrDefault(journalIdAsLong, setOfCurrentAuthors);
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
