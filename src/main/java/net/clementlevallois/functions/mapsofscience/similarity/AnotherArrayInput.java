package net.clementlevallois.functions.mapsofscience.similarity;

import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectLinkedOpenHashSet;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import net.clementlevallois.functions.mapsofscience.Constants;

import net.clementlevallois.utils.Clock;

public class AnotherArrayInput implements Input {

    Path pathJournalIdMapping = Path.of("data/journal-id-mapping.txt");
    Path pathAuthorIdMapping = Path.of("data/author-id-mapping.txt");
    Long2ObjectOpenHashMap<ObjectLinkedOpenHashSet<Long>> journal2AuthorsMap = new Long2ObjectOpenHashMap<>();
    List<String> allJournalsAndTheirAuthors;
    
    int[] data;
    int[] journals;
    
    Long2IntOpenHashMap mapJournals = new Long2IntOpenHashMap();
    Long2IntOpenHashMap mapAuthors = new Long2IntOpenHashMap();

    public static AnotherArrayInput fromFile(Path inputFilePath, Integer maxSize) throws IOException {
        AnotherArrayInput input = new AnotherArrayInput();
        input.parseFromFile(inputFilePath, maxSize);
        return input;
    }

    private void parseFromFile(Path pathJournalsToAuthors, Integer maxSize) throws IOException {

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

        clock = new Clock("measuring the length of the array we need");
        allJournalsAndTheirAuthors = Files.readAllLines(pathJournalsToAuthors);
        int cutOff = Math.min(allJournalsAndTheirAuthors.size(), maxSize);
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
        journals = new int[cutOff];
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

    private void processLine(String line) {
        String fields[] = line.split("\\|");
        if (fields.length < 2) {
            return;
        }
        String journalId = fields[0];
        long journalIdAsLong = Long.parseLong(journalId);
        String authorIdsAsLine = fields[1];
        String authorIds[] = authorIdsAsLine.split(",");
        ObjectLinkedOpenHashSet<Long> setOfCurrentAuthors = new ObjectLinkedOpenHashSet();
        ObjectLinkedOpenHashSet<Long> setOfAuthorsForThisJournal = journal2AuthorsMap.getOrDefault(journalIdAsLong,
                setOfCurrentAuthors);
        for (String authorId : authorIds) {
            try {
                long authorIdLong = Long.parseLong(authorId);
                setOfAuthorsForThisJournal.add(authorIdLong);
            } catch (NumberFormatException e) {
                System.out.println(line);
                System.out.println("error with author id, not  long: " + authorId);
            }
        }
        journal2AuthorsMap.put(journalIdAsLong, setOfAuthorsForThisJournal);
    }

    @Override
    public int size() {
        return journals.length;
    }
}
