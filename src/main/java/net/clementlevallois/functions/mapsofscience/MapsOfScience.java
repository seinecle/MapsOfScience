/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 */
package net.clementlevallois.functions.mapsofscience;

import io.mikael.urlbuilder.UrlBuilder;
import jakarta.json.Json;
import jakarta.json.JsonArray;
import jakarta.json.JsonObject;
import jakarta.json.JsonReader;
import jakarta.json.JsonValue;
import java.io.IOException;
import java.io.StringReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.DecimalFormat;
import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import static java.util.stream.Collectors.joining;
import java.util.stream.IntStream;
import net.clementlevallois.utils.Clock;

/**
 *
 * @author LEVALLOIS
 */
public class MapsOfScience {

    private int totalCountJournals = 0;
    private int totalCountAuthors = 0;
    private int totalCountWorks;
    private static final Semaphore semaphore = new Semaphore(5);
    private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    private final int PAGE_SIZE = 200;
    private final int LOGICAL_MAX_SEARCH = 2;
    private final String BETWEEN_FIELDS_SEPARATOR = ":::";
    private final String WITHIN_FIELD_SEPARATOR = "===";
    private final String REPLACEMENT_CHAR_FOR_SANITIZATION = "-";

    public static void main(String[] args) throws IOException, InterruptedException {
        MapsOfScience mos = new MapsOfScience();

//        mos.getAllJournalsFromAllAuthors();
//        mos.getJournalsCount();
//        mos.getAuthorsCount();
//        mos.getWorksCount();
        mos.getAllWorksPagedWithCursor();
//        mos.getAllAuthorsPagedWithCursor();
//        mos.getAllJournalsPagedWithCursor();
    }

    private void getJournalsCount() throws IOException, InterruptedException {
        URI uri = UrlBuilder
                .empty()
                .withScheme("https")
                .withHost("api.openalex.org")
                .withPath("sources")
                .addParameter("filter", "type:journal,works_count:>50")
                .addParameter("select", "id,display_name")
                .addParameter("mailto", "analysis@exploreyourdata.com")
                .toUri();

        HttpRequest request = HttpRequest.newBuilder()
                .uri(uri)
                .build();
        HttpClient client = HttpClient.newHttpClient();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() == 200) {
            String bodyString = response.body();
            try (JsonReader reader = Json.createReader(new StringReader(bodyString))) {
                JsonObject jsonObjectReturned = reader.readObject();
                totalCountWorks = jsonObjectReturned.getJsonObject("meta").getInt("count");
                System.out.println("total number of works in OpenAlex: " + totalCountWorks);
            }
        } else {
            System.out.println("error when retrieving initial count of journals");
        }
    }

    private void getWorksCount() throws IOException, InterruptedException {
        URI uri = UrlBuilder
                .empty()
                .withScheme("https")
                .withHost("api.openalex.org")
                .withPath("works")
                .addParameter("filter", "publication_year:>2014,cited_by_count:>0,type:journal-article")
                .addParameter("mailto", "analysis@exploreyourdata.com")
                .toUri();

        HttpRequest request = HttpRequest.newBuilder()
                .uri(uri)
                .build();
        HttpClient client = HttpClient.newHttpClient();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() == 200) {
            String bodyString = response.body();
            try (JsonReader reader = Json.createReader(new StringReader(bodyString))) {
                JsonObject jsonObjectReturned = reader.readObject();
                totalCountWorks = jsonObjectReturned.getJsonObject("meta").getInt("count");
                System.out.println("total number of works in OpenAlex: " + totalCountWorks);
            }
        } else {
            System.out.println("error when retrieving initial count of journals");
        }
    }

    private void getAuthorsCount() throws IOException, InterruptedException {
        URI uri = UrlBuilder
                .empty()
                .withScheme("https")
                .withHost("api.openalex.org")
                .withPath("authors")
                .addParameter("filter", "works_count:>4,cited_by_count:>10")
                .addParameter("select", "id,display_name")
                .addParameter("mailto", "analysis@exploreyourdata.com")
                .toUri();

        HttpRequest request = HttpRequest.newBuilder()
                .uri(uri)
                .build();
        HttpClient client = HttpClient.newHttpClient();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() == 200) {
            String bodyString = response.body();
            try (JsonReader reader = Json.createReader(new StringReader(bodyString))) {
                JsonObject jsonObjectReturned = reader.readObject();
                totalCountAuthors = jsonObjectReturned.getJsonObject("meta").getInt("count");
                System.out.println("total number of authors in OpenAlex: " + totalCountAuthors);
            }
        } else {
            System.out.println("error when retrieving initial count of authors");
        }
    }

    public void getAllJournalsPaged() throws IOException, InterruptedException {

        int numberOfPage = Math.round(totalCountJournals / PAGE_SIZE);

        System.out.println("number of pages: " + numberOfPage);

        int leftOver = totalCountJournals % PAGE_SIZE;

        if (leftOver > 0) {
            numberOfPage++;
        }

        int currPageIndex;
        HttpClient client = HttpClient.newHttpClient();

        Queue<JsonObject> queue = new ConcurrentLinkedQueue();

        for (currPageIndex = 1; currPageIndex <= numberOfPage; currPageIndex++) {
            semaphore.acquire();
            URI uri = UrlBuilder
                    .empty()
                    .withScheme("https")
                    .withHost("api.openalex.org")
                    .withPath("sources")
                    .addParameter("filter", "type:journal,works_count:>50")
                    .addParameter("select", "id,display_name")
                    .addParameter("per-page", String.valueOf(PAGE_SIZE))
                    .addParameter("page", String.valueOf(currPageIndex))
                    .addParameter("mailto", "analysis@exploreyourdata.com")
                    .toUri();

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(uri)
                    .timeout(Duration.ofSeconds(5))
                    .build();

            CompletableFuture<HttpResponse<String>> future = client.sendAsync(request, HttpResponse.BodyHandlers.ofString());
            future.thenAcceptAsync(resp -> {
                semaphore.release();
                if (resp.statusCode() == 200) {
                    String bodyString = resp.body();
                    try (JsonReader reader = Json.createReader(new StringReader(bodyString))) {
                        JsonObject jsonObjectReturned = reader.readObject();
                        JsonArray jsonArrayResults = jsonObjectReturned.getJsonArray("results");
                        Iterator<JsonValue> iteratorResults = jsonArrayResults.iterator();
                        while (iteratorResults.hasNext()) {
                            JsonObject nextResult = iteratorResults.next().asJsonObject();
                            queue.add(nextResult);
                        }
                    }
                } else {
                    System.out.println("status code: " + resp.statusCode());
                    System.out.println(resp.body());
                }
            },
                    scheduler);
            Thread.sleep(200);
        }
        scheduler.shutdown();

        StringBuilder sb = new StringBuilder();

        Iterator<JsonObject> iteratorQueue = queue.iterator();
        while (iteratorQueue.hasNext()) {
            JsonObject next = iteratorQueue.next();
            String journal = sanitize(next.getString("display_name"));
            sb.append(next.get("id")).append(BETWEEN_FIELDS_SEPARATOR).append(journal);
            sb.append("\n");
        }
        Files.writeString(Path.of("all-journals.txt"), sb.toString());
    }

    public void getAllJournalsPagedWithCursor() throws IOException, InterruptedException {
        int numberOfPage = Math.round(totalCountJournals / PAGE_SIZE);

        System.out.println("number of pages: " + numberOfPage);

        int leftOver = totalCountJournals % PAGE_SIZE;

        if (leftOver > 0) {
            numberOfPage++;
        }

        int currPageIndex;
        HttpClient client = HttpClient.newHttpClient();

        Queue<JsonObject> queue = new ConcurrentLinkedQueue();

        String cursor = "*";
        for (currPageIndex = 1; currPageIndex <= numberOfPage; currPageIndex++) {
            URI uri = UrlBuilder
                    .empty()
                    .withScheme("https")
                    .withHost("api.openalex.org")
                    .withPath("sources")
                    .addParameter("filter", "type:journal,works_count:>50")
                    .addParameter("select", "id,display_name")
                    .addParameter("per-page", String.valueOf(PAGE_SIZE))
                    .addParameter("cursor", cursor)
                    .addParameter("mailto", "analysis@exploreyourdata.com")
                    .toUri();

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(uri)
                    .timeout(Duration.ofSeconds(5))
                    .build();

            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                String bodyString = response.body();
                try (JsonReader reader = Json.createReader(new StringReader(bodyString))) {
                    JsonObject jsonObjectReturned = reader.readObject();
                    cursor = jsonObjectReturned.getJsonObject("meta").getString("next_cursor");
                    JsonArray jsonArrayResults = jsonObjectReturned.getJsonArray("results");
                    Iterator<JsonValue> iteratorResults = jsonArrayResults.iterator();
                    while (iteratorResults.hasNext()) {
                        JsonObject nextResult = iteratorResults.next().asJsonObject();
                        queue.add(nextResult);
                    }
                }
            } else {
                System.out.println("status code: " + response.statusCode());
                System.out.println(response.body());
            }
            TimeUnit.MILLISECONDS.sleep(110);
        }

        StringBuilder sb = new StringBuilder();

        Iterator<JsonObject> iteratorQueue = queue.iterator();

        while (iteratorQueue.hasNext()) {
            JsonObject next = iteratorQueue.next();
            sb.append(next.get("id")).append(BETWEEN_FIELDS_SEPARATOR).append(sanitize(next.getString("display_name")));
            sb.append("\n");
        }

        Files.writeString(Path.of("all-journals.txt"), sb.toString());
    }

    public void getAllAuthorsPagedWithCursor() throws IOException, InterruptedException {
        Clock clock = new Clock("global clock");
        DecimalFormat df = new DecimalFormat("0.00");
        int numberOfPages = Math.round(totalCountAuthors / PAGE_SIZE);

        System.out.println("number of pages to fetch: " + numberOfPages);

        int leftOver = totalCountAuthors % PAGE_SIZE;

        if (leftOver > 0) {
            numberOfPages++;
        }

        int currPageIndex;
        HttpClient client = HttpClient.newHttpClient();

        Queue<JsonObject> queue = new ConcurrentLinkedQueue();

        String cursor = "*";
        for (currPageIndex = 1; currPageIndex <= numberOfPages; currPageIndex++) {
            URI uri = UrlBuilder
                    .empty()
                    .withScheme("https")
                    .withHost("api.openalex.org")
                    .withPath("authors")
                    .addParameter("filter", "works_count:>1,has_orcid:true,cited_by_count:>1")
                    .addParameter("select", "orcid")
                    .addParameter("per-page", String.valueOf(PAGE_SIZE))
                    .addParameter("cursor", cursor)
                    .addParameter("mailto", "analysis@exploreyourdata.com")
                    .toUri();

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(uri)
                    .timeout(Duration.ofSeconds(25))
                    .build();

            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                String bodyString = response.body();
                try (JsonReader reader = Json.createReader(new StringReader(bodyString))) {
                    JsonObject jsonObjectReturned = reader.readObject();
                    cursor = jsonObjectReturned.getJsonObject("meta").getString("next_cursor");
                    JsonArray jsonArrayResults = jsonObjectReturned.getJsonArray("results");
                    Iterator<JsonValue> iteratorResults = jsonArrayResults.iterator();
                    while (iteratorResults.hasNext()) {
                        JsonObject nextResult = iteratorResults.next().asJsonObject();
                        queue.add(nextResult);
                    }
                }
            } else {
                System.out.println("status code: " + response.statusCode());
                System.out.println(response.body());
            }
            TimeUnit.MILLISECONDS.sleep(110);
            if (currPageIndex % 100 == 0) {
                System.out.print("page " + currPageIndex + " out of " + numberOfPages);
                float percentage = (float) currPageIndex / numberOfPages * 100;
                System.out.println(" (" + df.format(percentage) + "%)");
            }
        }

        StringBuilder sb = new StringBuilder();

        Iterator<JsonObject> iteratorQueue = queue.iterator();

        while (iteratorQueue.hasNext()) {
            JsonObject next = iteratorQueue.next();
            sb.append(next.get("orcid"));
            sb.append("\n");
        }

        Files.writeString(Path.of("all-authors.txt"), sb.toString());
        clock.closeAndPrintClock();
    }

    public void getAllJournalsFromAllAuthors() throws IOException, InterruptedException {
        Clock clock = new Clock("global clock");
        if (!Path.of("temp").toFile().exists()) {
            Files.createDirectories(Path.of("temp"));
        }
        DecimalFormat df = new DecimalFormat("0.00");
        int numberOfPages = Math.round(totalCountAuthors / PAGE_SIZE);

        System.out.println("number of pages to fetch: " + numberOfPages);

        int leftOver = totalCountAuthors % PAGE_SIZE;

        if (leftOver > 0) {
            numberOfPages++;
        }

        numberOfPages = 10;

        int currPageIndex = 0;
        HttpClient client = HttpClient.newHttpClient();

        Queue<String> queue = new ConcurrentLinkedQueue();

        String cursor = "*";
        while (cursor != null) {
            URI uri = UrlBuilder
                    .empty()
                    .withScheme("https")
                    .withHost("api.openalex.org")
                    .withPath("authors")
                    .addParameter("filter", "works_count:>3,cited_by_count:>5")
                    .addParameter("select", "id")
                    .addParameter("per-page", String.valueOf(PAGE_SIZE))
                    .addParameter("cursor", cursor)
                    .addParameter("mailto", "analysis@exploreyourdata.com")
                    .toUri();

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(uri)
                    .timeout(Duration.ofSeconds(25))
                    .build();

            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                String bodyString = response.body();
                try (JsonReader reader = Json.createReader(new StringReader(bodyString))) {
                    JsonObject jsonObjectReturned = reader.readObject();
                    cursor = jsonObjectReturned.getJsonObject("meta").getString("next_cursor");
                    JsonArray jsonArrayResults = jsonObjectReturned.getJsonArray("results");
                    Collection<List<String>> partitionedList = IntStream.range(0, jsonArrayResults.size())
                            .boxed()
                            .collect(Collectors.groupingBy(partition -> (partition / LOGICAL_MAX_SEARCH), Collectors.mapping(elementIndex -> jsonArrayResults.getJsonObject(elementIndex).getString("id"), Collectors.toList())))
                            .values();
                    for (List<String> partition : partitionedList) {
                        String journalsForGivenAuthor = getJournalsForListOfAuthors(partition);
                        if (journalsForGivenAuthor != null && !journalsForGivenAuthor.isBlank()) {
                            queue.add(journalsForGivenAuthor);
                        }
                    }

                }
            } else {
                System.out.println("status code: " + response.statusCode());
                System.out.println(response.body());
            }
            currPageIndex++;
            TimeUnit.MILLISECONDS.sleep(110);
            if (currPageIndex % 50 == 0) {
                System.out.print("page " + currPageIndex + " out of " + numberOfPages);
                float percentage = (float) currPageIndex / numberOfPages * 100;
                System.out.println(" (" + df.format(percentage) + "%)");
                String join = String.join("\n", queue);
                Files.writeString(Path.of("temp", currPageIndex + ".txt"), join);
            }
        }

        clock.closeAndPrintClock();
    }

    public String getJournalsForGivenAuthor(String authorOpenAlexId) {
        Set<String> journalIds = new HashSet();
        try {
            URI uri = UrlBuilder
                    .empty()
                    .withScheme("https")
                    .withHost("api.openalex.org")
                    .withPath("works")
                    .addParameter("filter", "author.id:" + authorOpenAlexId + ",primary_location.source.type:journal")
                    .addParameter("select", "primary_location")
                    .addParameter("per-page", "200")
                    .addParameter("sort", "publication_year:desc")
                    .addParameter("mailto", "analysis@exploreyourdata.com")
                    .toUri();

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(uri)
                    .build();
            HttpClient client = HttpClient.newHttpClient();

            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                String bodyString = response.body();
                try (JsonReader reader = Json.createReader(new StringReader(bodyString))) {
                    JsonObject jsonObjectReturned = reader.readObject();
                    JsonArray jsonArrayResults = jsonObjectReturned.getJsonArray("results");
                    Iterator<JsonValue> iteratorResults = jsonArrayResults.iterator();
                    while (iteratorResults.hasNext()) {
                        JsonObject nextResult = iteratorResults.next().asJsonObject();
                        JsonObject primaryLocation = nextResult.getJsonObject("primary_location");
                        if (primaryLocation != null && primaryLocation.containsKey("source")) {
                            String journalId = primaryLocation.getJsonObject("source").getString("id");
                            journalIds.add(sanitize(journalId));
                        }
                    }
                }
                if (!journalIds.isEmpty()) {
                    return String.join(WITHIN_FIELD_SEPARATOR, journalIds);
                } else {
                    return "";
                }
            } else {
                System.out.println(response.body());
                System.out.println("error when retrieving works of an author");
            }
        } catch (IOException | InterruptedException ex) {
            Logger.getLogger(MapsOfScience.class.getName()).log(Level.SEVERE, null, ex);
        }
        return "";
    }

    public String getJournalsForListOfAuthors(List<String> authorOpenAlexIds) {
        Set<String> journalIds = new HashSet();
        String idsToFetch = authorOpenAlexIds.stream().map(openAlexId -> {
            String[] openAlexIdParts = openAlexId.split("/");
            int length = openAlexIdParts.length;
            return openAlexIdParts[length - 1];
        }).collect(joining("|"));
        try {
            URI uri = UrlBuilder
                    .empty()
                    .withScheme("https")
                    .withHost("api.openalex.org")
                    .withPath("works")
                    .addParameter("filter", "author.id:" + idsToFetch + ",primary_location.source.type:journal")
                    .addParameter("select", "primary_location")
                    .addParameter("per-page", "200")
                    .addParameter("sort", "publication_year:desc")
                    .addParameter("mailto", "analysis@exploreyourdata.com")
                    .toUri();

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(uri)
                    .build();
            HttpClient client = HttpClient.newHttpClient();

            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                String bodyString = response.body();
                try (JsonReader reader = Json.createReader(new StringReader(bodyString))) {
                    JsonObject jsonObjectReturned = reader.readObject();
                    JsonArray jsonArrayResults = jsonObjectReturned.getJsonArray("results");
                    Iterator<JsonValue> iteratorResults = jsonArrayResults.iterator();
                    while (iteratorResults.hasNext()) {
                        JsonObject nextResult = iteratorResults.next().asJsonObject();
                        JsonObject primaryLocation = nextResult.getJsonObject("primary_location");
                        if (primaryLocation != null && primaryLocation.containsKey("source")) {
                            String journalId = primaryLocation.getJsonObject("source").getString("id");
                            journalIds.add(sanitize(journalId));
                        }
                    }
                }
                if (!journalIds.isEmpty()) {
                    return String.join(WITHIN_FIELD_SEPARATOR, journalIds);
                } else {
                    return "";
                }
            } else {
                System.out.println(response.body());
                System.out.println("error when retrieving works of an author");
            }
        } catch (IOException | InterruptedException ex) {
            Logger.getLogger(MapsOfScience.class.getName()).log(Level.SEVERE, null, ex);
        }
        return "";
    }

    public void getAllWorksPagedWithCursor() throws InterruptedException{
        Clock clock = new Clock("global clock");
        HttpClient client = HttpClient.newHttpClient();

        ConcurrentLinkedQueue<String> queue = new ConcurrentLinkedQueue();
        int writeToDiskIntervalInSeconds = 30;
        JsonQueueProcessor queueProcessor = new JsonQueueProcessor(Path.of("all-works.json"), queue, writeToDiskIntervalInSeconds);
        Thread queueProcessorThread = new Thread(queueProcessor);
        queueProcessorThread.start();

        int currPageIndex = 0;

        String cursor = "*";
        while (!cursor.equals("done")) {
            URI uri = UrlBuilder
                    .empty()
                    .withScheme("https")
                    .withHost("api.openalex.org")
                    .withPath("works")
                    .addParameter("filter", "publication_year:>2014,cited_by_count:>0,type:journal-article")
                    .addParameter("select", "authorships, primary_location")
                    .addParameter("per-page", String.valueOf(PAGE_SIZE))
                    .addParameter("cursor", cursor)
                    .addParameter("mailto", "analysis@exploreyourdata.com")
                    .toUri();

            HttpRequest request = HttpRequest.newBuilder().uri(uri).timeout(Duration.ofSeconds(25)).build();

            try {
                HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

                if (response.statusCode() == 200) {
                    String bodyString = response.body();
                    try (JsonReader reader = Json.createReader(new StringReader(bodyString))) {
                        JsonObject jsonObjectReturned = reader.readObject();
                        cursor = jsonObjectReturned.getJsonObject("meta").getString("next_cursor","done");
                        JsonArray jsonArrayResults = jsonObjectReturned.getJsonArray("results");
                        Iterator<JsonValue> iteratorResults = jsonArrayResults.iterator();
                        while (iteratorResults.hasNext()) {
                            JsonObject nextResult = iteratorResults.next().asJsonObject();
                            queue.add(nextResult.toString());
                        }
                    }
                } else {
                    System.out.println("status code: " + response.statusCode());
                    System.out.println(response.body());
                }
            } catch (HttpTimeoutException e) {
                System.err.println("HTTP request timed out for page " + currPageIndex);
            } catch (IOException | InterruptedException e) {
                System.err.println("HTTP request failed: " + e.getMessage());
            }

            TimeUnit.MILLISECONDS.sleep(110);
            if (currPageIndex % 100 == 0) {
                System.out.println("page " + currPageIndex);
            }
            currPageIndex++;
        }

        queueProcessor.stop();

        queueProcessorThread.interrupt();

        clock.closeAndPrintClock();
    }

    public String sanitize(String input) {
        String output = input.replaceAll(BETWEEN_FIELDS_SEPARATOR, REPLACEMENT_CHAR_FOR_SANITIZATION);
        output = output.replaceAll(WITHIN_FIELD_SEPARATOR, REPLACEMENT_CHAR_FOR_SANITIZATION);

        return output;
    }

}
