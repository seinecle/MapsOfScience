package net.clementlevallois.functions.mapsofscience.similarity;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.openide.util.Exceptions;

public class AnotherArraySimilarityComputer implements SimilarityComputer {

    private final AnotherArrayInput input;
    private final ResultWriter writer;
    int[] data;
    int[] journals;

    private final Thread thread;

    public AnotherArraySimilarityComputer(AnotherArrayInput input, ResultWriter writer, Integer jobs) {
        this.input = input;
        this.writer = writer;
        this.data = input.data;
        this.journals = input.journals;

        thread = new Thread(() -> process());
        thread.start();
    }

    @Override
    public void join() throws InterruptedException {
        thread.join();
    }

    private void process() {
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

                // as long as we don't hit the end of the array...
                while (indexSecondJournalInDataArray < data.length) {
                    // compute the similarities between the 2 journals
                    int similarity = pairwiseComparison(indexFirstJournalInDataArray, indexSecondJournalInDataArray);
                    if (similarity > 0) {
                        // this is the step where a similarity btw 2 journals has been found and it is offloaded to this queue.
                        writer.add(String.valueOf(data[indexFirstJournalInDataArray]), String.valueOf(data[indexSecondJournalInDataArray]), similarity);
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
        try {
            //Awaits either 1 minute or if all tasks are completed. Whatever is first.
            executor.awaitTermination(1L, TimeUnit.MINUTES);
        } catch (InterruptedException ex) {
            Exceptions.printStackTrace(ex);
        }
    }

    private int pairwiseComparison(int first, int second) {
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
