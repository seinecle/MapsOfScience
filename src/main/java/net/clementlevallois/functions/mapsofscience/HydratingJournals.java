/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Main.java to edit this template
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
import java.io.StringWriter;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.gephi.graph.api.GraphController;
import org.gephi.graph.api.GraphModel;
import org.gephi.graph.api.Node;
import org.gephi.graph.api.NodeIterable;
import org.gephi.io.exporter.api.ExportController;
import org.gephi.io.exporter.plugin.ExporterGEXF;
import org.gephi.io.importer.api.Container;
import org.gephi.io.importer.api.ContainerUnloader;
import org.gephi.io.importer.api.ImportController;
import org.gephi.io.importer.plugin.file.ImporterGEXF;
import org.gephi.io.importer.spi.FileImporter;
import org.gephi.io.processor.plugin.DefaultProcessor;
import org.gephi.project.api.ProjectController;
import org.openide.util.Lookup;

/**
 *
 * @author LEVALLOIS
 */
public class HydratingJournals {

    /**
     * @param args the command line arguments
     */
    URI uri;
    HttpRequest request;
    HttpClient client = HttpClient.newHttpClient();

    HttpResponse<String> response;
    Map<String, String> integerId2OpenAlexId = new HashMap();
    Map<String, String> openAlexId2IntegerIds = new HashMap();
    Map<String, String> integerId2JournalName = new HashMap();

    public static void main(String[] args) throws IOException, InterruptedException {
        Path gexfPath = Path.of("C:\\Users\\levallois\\open\\nocode-app-functions\\MapsOfScience\\data\\maps\\20230607\\edges-removed.gexf");
        HydratingJournals hj = new HydratingJournals();
        hj.getJournalNames();
        hj.writeJournalNames();
        hj.addJournalNamesToGexf(gexfPath);
    }

    public void getJournalNames() throws IOException, InterruptedException {

        List<String> journalIdsMapped = Files.readAllLines(Path.of("C:\\Users\\levallois\\open\\nocode-app-functions\\MapsOfScience\\data\\journal-id-mapping.txt"));

        for (String line : journalIdsMapped) {
            String[] fields = line.split(",");
            integerId2OpenAlexId.put(fields[1], fields[0]);
            openAlexId2IntegerIds.put(fields[0], fields[1]);
        }

        int countIds = 1;
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, String> entry : integerId2OpenAlexId.entrySet()) {
            String openAlexId = entry.getValue();
            sb.append("https://openalex.org/S").append(openAlexId);
            if (countIds < 50) {
                sb.append("|");
            }
            if (countIds++ == 50) {
                callOpenAlex(sb.toString());
                sb = new StringBuilder();
                countIds = 1;
                Thread.sleep(100);
            }
        }
    }

    public void writeJournalNames() throws IOException {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, String> entry : integerId2JournalName.entrySet()) {
            sb.append(entry.getKey()).append("|").append(entry.getValue()).append(System.lineSeparator());
        }
        Files.writeString(Path.of("integer-ids-to-journal-names.txt"), sb.toString(), StandardCharsets.UTF_8);
    }

    public void callOpenAlex(String idsToFetch) throws IOException, InterruptedException {
        uri = UrlBuilder
                .empty()
                .withScheme("https")
                .withHost("api.openalex.org")
                .withPath("sources")
                .withQuery("filter=openalex:" + idsToFetch)
                .toUri();
//        System.out.println("uri: " + uri.toString());

        request = HttpRequest.newBuilder()
                .uri(uri)
                .build();

        response = client.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() == 200) {
            String bodyString = response.body();
            try (JsonReader reader = Json.createReader(new StringReader(bodyString))) {
                JsonObject jsonObjectReturned = reader.readObject();
                JsonArray jsonArrayResults = jsonObjectReturned.getJsonArray("results");
                Iterator<JsonValue> iteratorResults = jsonArrayResults.iterator();
                while (iteratorResults.hasNext()) {
                    JsonObject nextResult = iteratorResults.next().asJsonObject();
                    String journalName = nextResult.getString("display_name");
                    String id = nextResult.getString("id");
                    String openAlexId = keepLastPartOfId(id);
                    String integerId = openAlexId2IntegerIds.get(openAlexId);
                    integerId2JournalName.put(integerId, journalName);
                }
            }
        } else {
            String bodyString = response.body();
            System.out.println("error when retrieving name of journal");
            System.out.println(bodyString);
        }
    }

    public void addJournalNamesToGexf(Path gexfPath) throws IOException {

        String gexf = Files.readString(gexfPath);
        ProjectController projectController = Lookup.getDefault().lookup(ProjectController.class);
        GraphController graphController = Lookup.getDefault().lookup(GraphController.class);
        ImportController importController = Lookup.getDefault().lookup(ImportController.class);

        projectController.newProject();
        Container container = null;

        FileImporter fi = new ImporterGEXF();
        container = importController.importFile(new StringReader(gexf), fi);

        container.closeLoader();

        DefaultProcessor processor = new DefaultProcessor();

        processor.setWorkspace(projectController.getCurrentWorkspace());
        processor.setContainers(new ContainerUnloader[]{container.getUnloader()}
        );
        processor.process();
        GraphModel gm = graphController.getGraphModel();

        NodeIterable nodes = gm.getGraph().getNodes();

        for (Node node : nodes) {
            String journalName = integerId2JournalName.get((String) node.getId());
            node.setLabel(journalName);
        }

        ExportController ec = Lookup.getDefault().lookup(ExportController.class);
        ExporterGEXF exporterGexf = (ExporterGEXF) ec.getExporter("gexf");

        exporterGexf.setWorkspace(projectController.getCurrentWorkspace());
        exporterGexf.setExportDynamic(false);
        exporterGexf.setExportPosition(true);
        exporterGexf.setExportSize(true);
        exporterGexf.setExportColors(true);
        exporterGexf.setExportMeta(true);

        StringWriter stringWriter = new StringWriter();

        ec.exportWriter(stringWriter, exporterGexf);

        stringWriter.close();
        String resultGexf = stringWriter.toString();

        Files.writeString(Path.of("journal-map-with-names.gexf"), resultGexf);

    }

    private static String keepLastPartOfId(String fullId) {
        String[] idFields = fullId.split("/");
        String idWithLetter = idFields[idFields.length - 1];
        return idWithLetter.substring(1);
    }
}
