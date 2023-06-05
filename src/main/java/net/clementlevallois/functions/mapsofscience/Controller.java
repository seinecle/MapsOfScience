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

        
        /**
         * The works we received from OpenAlex are json objects.
         * We will stored them in a txt file as an array of json objects.
         * So, we need first to create this empty txt file with an opening '[' characters.
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
         * Adds a couple of characters to the txt file to close the json array.
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
         */
        CreateListOfJournalsWithTheirAuthors createList = new CreateListOfJournalsWithTheirAuthors();
        createList.doAllOps(journalsAndAuthorsPerWork, journalsAndAuthors, journalIdFileString, authorIdFileString);

          
          
    }
}
