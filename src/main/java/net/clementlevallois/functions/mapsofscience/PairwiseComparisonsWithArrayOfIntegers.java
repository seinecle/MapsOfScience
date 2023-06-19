/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package net.clementlevallois.functions.mapsofscience;

import net.clementlevallois.functions.mapsofscience.queueprocessors.ArraysOfIntegerQueueProcessor;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import net.clementlevallois.utils.Clock;

/**
 *
 * @author LEVALLOIS
 */
public class PairwiseComparisonsWithArrayOfIntegers {

    static Path pathJournalIdMapping = Path.of("data/journal-id-mapping.txt");
    static Path pathAuthorIdMapping = Path.of("data/author-id-mapping.txt");
    static Path pathJournalsToAuthors = Path.of("data/all-journals-and-their-authors.txt");
    static Path pathSimilarities = Path.of("data/similarities-with-integers-at-least-2-no virtual-threads.csv");
    static Long2IntOpenHashMap mapJournals = new Long2IntOpenHashMap();
    static Long2IntOpenHashMap mapAuthors = new Long2IntOpenHashMap();

    static int[] data;
    static int[] journals;

    // method suggested by reddit user Ivory2Much:
    // https://www.reddit.com/r/java/comments/13rlb26/speeding_up_pairwise_comparisons_to_28_millionsec/
    public static void main(String args[]) throws IOException, InterruptedException, ExecutionException {
        boolean testInMain = false;
        int limit = Integer.MAX_VALUE - 10_000;

        if (args.length > 0) {
            limit = Integer.parseInt(args[0]);
            System.out.println("run limited to " + limit + " entities");
        } else {
            System.out.println("run not limited");
        }

        loadJournalAndAuthorIds();

        createBigIntegerArray(limit);
        if (testInMain) {
            data = new int[]{0, 3, 0, 5, 50, 1, 3, 2, 3, 5, 2, 5, 3, 6, 49, 50, 97, 3, 2, 30, 230};
            journals = new int[]{0, 5, 10, 17};
        }

        computeSimilarities(testInMain, pathSimilarities);
    }

    public static void loadJournalAndAuthorIds() throws IOException {

        /*
        
        This method loads in memory what we had stored in files in the previous operations:
        - the mapping of journal "original long ids" to their new integer ids indexed at zero, which are lighter to manipulate
        - same for author ids
        
         */
        Clock clock = new Clock("loading journal and author ids");
        List<String> allJournals = Files.readAllLines(pathJournalIdMapping);
        System.out.println("number of journals: " + allJournals.size());

        for (String string : allJournals) {
            String[] lineFields = string.split(",");
            mapJournals.put(Long.parseLong(lineFields[0]), Integer.parseInt(lineFields[1]));
        }
        List<String> allAuthors = Files.readAllLines(pathAuthorIdMapping);
        System.out.println("number of authors: " + allAuthors.size());
        for (String string : allAuthors) {
            String[] lineFields = string.split(",");
            mapAuthors.put(Long.parseLong(lineFields[0]), Integer.parseInt(lineFields[1]));
        }
        clock.closeAndPrintClock();
    }

    public static void createBigIntegerArray(int limit) throws IOException {

        /*
        
        This is the heart of the method. The basic data "one journal id, and its associated author ids" IS FLATTENED in a SINGLE array of integers.
       
        The format of this array is: [journal id 0, nb of authors for this journal, author associated to this journal, another author asoociated to this journal, journal id 1, nb of authors for his journal, etc]

        See the link above for a more rigorous example.
        
        The benefit of this method is that the entire operation will involved the manipulation of only a single (big!) array of integers.
        
        Double looping is still involved but all other computing costs are diminished to a minimum.
        
         */
        Clock clock = new Clock("measuring the length of the array we need");
        List<String> allJournalsAndTheirAuthors = Files.readAllLines(pathJournalsToAuthors);
        int cutOff = Math.min(allJournalsAndTheirAuthors.size(),limit);
        allJournalsAndTheirAuthors = allJournalsAndTheirAuthors.subList(0, cutOff);

        int count = 0;
        for (String string : allJournalsAndTheirAuthors) {
            String[] split = string.split("[\\" + Constants.INTER_FIELD_SEPARATOR + Constants.INTRA_FIELD_SEPARATOR + "]");
            count = count + split.length + 1;
        }
        System.out.println("length of the array we need: " + count);
        clock.closeAndPrintClock();

        clock = new Clock("initiating and filling the array");
        data = new int[count];

        // this "journals" array is a convenience array which stores the indices of all journals in the data[] array.
        // useful later to iterate through journals in the outer loop
        journals = new int[mapJournals.size()];
        int i = 0;
        int countJournals = 0;
        for (String string : allJournalsAndTheirAuthors) {

            // to recall, each line is in the form:
            // 45645464|54564561516,155161616516,
            // where the first element is the journal id, then a pipe character, then a comma-separated list of author ids.
            String[] elements = string.split("\\" + Constants.INTER_FIELD_SEPARATOR);
            String journalIdAsString = elements[0];
            String[] authorIdsAsString = elements[1].split(Constants.INTRA_FIELD_SEPARATOR);
            int journalId = mapJournals.get(Long.parseLong(journalIdAsString));

            // adding the index of the journal in the data array to a convenience array
            journals[countJournals++] = i;

            // adding the journal id to the array of ints
            data[i++] = journalId;

            // adding the number of authors to the array of ints
            // it is conveniently represented by the size of the array containing all author ids
            data[i++] = authorIdsAsString.length;

            // now we need to insert the list of all author ids in the int array, SORTED ASCENDING.
            // as they as stored as Longs represented in Strings,
            // we need to convert the Strings to Longs, then retrieve their int equivalent,
            // then sort the ints ascendingly before inserted them in the array! Phew.
            // the sublist thing is to remove the journal id, which is the first element of the array
            List<String> authorsAsStrings = Arrays.asList(authorIdsAsString);
            Set<Integer> authorsAsIntegers = new TreeSet();
            for (String authorIdAsString : authorsAsStrings) {
                long authorIdAsLong = Long.parseLong(authorIdAsString);
                int authorIdAsInteger = mapAuthors.get(authorIdAsLong);
                authorsAsIntegers.add(authorIdAsInteger);
            }
            for (int authorIdAsInteger : authorsAsIntegers) {
                data[i++] = authorIdAsInteger;
            }
        }
        clock.closeAndPrintClock();

    }

    public static void computeSimilarities(boolean test, Path result) throws InterruptedException, ExecutionException, IOException {
        Clock clock = new Clock("computing similarities");
        ConcurrentLinkedQueue<int[]> queue = new ConcurrentLinkedQueue();
        int writeToDiskIntervalInSeconds = 1;

        // this class is helpful to retrieve, and write to file the pairs of journals that do have a non zero similarity,
        // all while interrupting the least possible the computations on similartiies
        ArraysOfIntegerQueueProcessor queueProcessor = new ArraysOfIntegerQueueProcessor(result, queue, writeToDiskIntervalInSeconds);
        Thread queueProcessorThread = new Thread(queueProcessor);
        queueProcessorThread.start();

//        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        ExecutorService executor = Executors.newWorkStealingPool();

        int indexOfFirstJournalInJournals = 0;
        Runnable runnableTask;
        // we iterate through all journals

        while (indexOfFirstJournalInJournals < journals.length) {
            int indexFirstJournalInDataArray = journals[indexOfFirstJournalInJournals];
            int indexOfFirstJournalInJournalsAsLocalVariable = indexOfFirstJournalInJournals;
            runnableTask = () -> {
                /*
                
                how do we find the index of the second journal to compare the first journal to?
                Remember that a journal's info, up to the next journal, is stored as:
                
                [journal id, number of authors associated with it, author a, author b, ..., next journal id]
                
                basically, when we encounter the index of the first journal, we must go:
                - past the index where the cardinality of the number of authors associated with it is stored
                - past each of these authors, which are listed after the first journal id
                - and one more index to get to the second journal.
               
                                
                 */
//                int second = first + 1 + data[first + 1] + 1;
                int indexOfSecondJournalInJournals = indexOfFirstJournalInJournalsAsLocalVariable + 1;
                if (indexOfSecondJournalInJournals >= journals.length) {
                    return;
                }
                int indexSecondJournalInDataArray = journals[indexOfSecondJournalInJournals];
                int[] triplet;

                // as long as we don't hit the end of the array...
                while (indexSecondJournalInDataArray < data.length) {
                    // compute the similarities between the 2 journals
                    int similarity = pairwiseComparison(indexFirstJournalInDataArray, indexSecondJournalInDataArray);
                    if (similarity > 4) {
                        triplet = new int[3];
                        triplet[0] = data[indexFirstJournalInDataArray];
                        triplet[1] = data[indexSecondJournalInDataArray];
                        triplet[2] = similarity;
                        // this is the step where a similarity btw 2 journals has been found and it is offloaded to this queue.
                        queue.add(triplet);
                    }

                    /* and how do we move to the next journal to be compared to the first journal ?
                    
                    same logic as the logic we used to find this second journal, right above:
                    
                    - we take the index of the second journal, that we have just finished comparing to the first journal
                    - we move right by a number of indices which correspond to its number of associated authors. This number [the cardinality] is stored at [second +1]
                    - we add one indice to move past the cardinality as well, and one more to land on the next journal.

                     */
//                    second += data[second + 1] + 1 + 1;
                    indexOfSecondJournalInJournals++;
                    if (indexOfSecondJournalInJournals >= journals.length) {
                        return;
                    }
                    indexSecondJournalInDataArray = journals[indexOfSecondJournalInJournals];
                }

            };
            executor.execute(runnableTask);
            indexOfFirstJournalInJournals++;
        }
        executor.shutdown();
        //Awaits either 1 minute or if all tasks are completed. Whatever is first.
        executor.awaitTermination(1L, TimeUnit.MINUTES);

        queueProcessor.stop();
        clock.closeAndPrintClock();
    }

    public static int pairwiseComparison(int first, int second) {
        // indices of the last authors
        int firstJournalLastAuthorIndex = first + 1 + data[first + 1];
        int secondJournalLastAuthorIndex = second + 1 + data[second + 1];

        // indices of the first authors
        // (potentially beyond last, when 0 authors) <-- I have added checks to make sure there is always one author in the data
        // so that 'first + 2' or 'second + 2' lands on an author, not on the next journal's id.
        int f = first + 2;
        int s = second + 2;

        int matches = 0;

        // authors
        int fa = -1;
        int sa = -1;

        // this part I understood thanks to the explanations of ChatGPT: it consists in moving from index on both arrays as a quick way to count similar authors.
        while (f <= firstJournalLastAuthorIndex && s <= secondJournalLastAuthorIndex) {
            if (fa < 0) {
                fa = data[f];
            }
            if (sa < 0) {
                sa = data[s];
            }

            if (fa < sa) {
                f++;
                fa = -1;
            } else if (fa > sa) {
                s++;
                sa = -1;
            } else {
                matches++;
                f++;
                fa = -1;
                s++;
                sa = -1;
            }
        }
        return matches;
    }
}
