/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Main.java to edit this template
 */
package net.clementlevallois.functions.mapsofscience;

import java.io.IOException;

/**
 *
 * @author LEVALLOIS
 */
public class Controller {

    /**
     * @param args the command line arguments
     * @throws java.io.IOException
     * @throws java.lang.InterruptedException
     */
    public static void main(String[] args) throws IOException, InterruptedException, Exception {
        String worksFileString = "all-works-sample.json";
        String worksWellFormatted = "all-works-well-formatted.json";
        String journalsAndAuthorsPerWork = "journals-and-authors-per-work.txt";
        String journalsAndAuthors = "all-journals-and-their-authors.txt";
        String journalIdFileString = "journal-id-mapping.txt";
        String authorIdFileString = "author-id-mapping.txt";
        
        boolean testOnSmallSample = false;

        
        // THESE STEPS CAN RUN ON A COMPUTER WITH 4Gb or RAM and 100Gb of free disk space.
        
        // PREFERABLY A SERVER AS DATA COLLECTION TAKES 20 hours, BUT A LAPTOP WOULD WORK AS WELL
        
        // ++++ CAREFUL, THE LAST STEP DESCRIBED BELOW NEEDS A COMPUTER WITH 12GB OF RAM. +++
        
        /**
         * The works we are going to receive from OpenAlex are json objects.
         * We will store them in a txt file as an array of json objects.
         * So, we need first to create this empty txt file with an opening '[' character, which is the opening char for an array
         */
        
        OperationsOnJson jsonOps = new OperationsOnJson();
        jsonOps.addOpeningCharsToJsonArray(worksFileString);

        /**
         * Retrieving works from OpenAlex.
         * Takes about 20 hours and 96,000 API calls. Generates a 67Gb file.
         */
        APICallsToOpenAlex apiCalls = new APICallsToOpenAlex(testOnSmallSample);
        apiCalls.getAllWorksPagedWithCursor(worksFileString);

        /**
         * Deletes the final comma and adds a closing ']' to the txt file to close the json array.
         */
        jsonOps.addClosingCharsToJsonArray(worksFileString, worksWellFormatted);

        /**
         * Filters the json file to retain only what we need: journal ids and
         * author ids. Takes approx 10 minutes for 20 million works, which is a
         * 65Gb file. Returns a file of 1.2Gb approx.
         */
        JsonStreamingParser parser = new JsonStreamingParser();
        parser.parseJournalIdsAndAuthorIds(worksWellFormatted, journalsAndAuthorsPerWork);

        /**
         * Reorganizes journal ids and author ids to end up with: for each line,
         * a journal id followed by all author ids who have published in it.
         * Takes 3 to 10 minutes and needs 4GB of max heap size (add a -Xmx4g
         * paramater in the command line launching the java program). Produces a
         * file close to 1Gb in size.
         * 
         * Also, this step now also produces two files:
         * - one file where each journal id is remapped to ids in the form of a sequence of 0, 1, 2...
         * - one file where each author id is remapped to ids in the form of a sequence of 0, 1, 2...
         * 
         * This is necessary because in the next step, we will use the method suggested by reddit user Ivory2Match to do pairwise comparisons of journals
         * This method involves manipulating journal ids and author ids in  single big array of integers. For this reason, we turn original ids, which are longs, into ints.
         * 
         */
        CreateListOfJournalsWithTheirAuthors createList = new CreateListOfJournalsWithTheirAuthors();
        createList.doAllOps(journalsAndAuthorsPerWork, journalsAndAuthors, journalIdFileString, authorIdFileString);

        /*

        THIS LAST STEP NEEDS A LAPTOP OR SERVER WITH 12GB OF RAM BECAUSE ALL THE DATA IS LOADED IN MEMORY
        
        The last step is comparing each journal to each other and connect them if they have at least one author in common.
        
        This step is performed using the files generated so far.
        
                
        The initial approach used is in JournalSimilaritiesComputer.java
        I need to test it again, last time it gave me an out of memory error but I think it was just due to my error in setting the max heap size too low.
        
        SINCE THEN I have received a suggestion by reddit user Ivory2Much for an alternative approach, which is implemented in PairwiseComparisonsWithArrayOfIntegers.java
        The approach: https://www.reddit.com/r/java/comments/13rlb26/speeding_up_pairwise_comparisons_to_28_millionsec/jlm0me1/
        
        Please refer to this file "PairwiseComparisonsWithArrayOfIntegers.java" where I have added extensive explanations on this approach,
        which is both very simple (one single array of integers!) and pretty smart in its logic.

        I have not tested it yet.
        
        */
          
    }
}
