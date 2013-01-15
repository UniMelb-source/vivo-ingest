package au.edu.unimelb.rdr;

import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.Syntax;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.ModelMaker;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.sdb.SDBFactory;
import com.hp.hpl.jena.sdb.Store;
import com.hp.hpl.jena.util.FileManager;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.StringWriter;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class RDFController {

    private Model remoteModel;
    private Model localModel;
    private String remoteModelName;
    private String localModelName;
    private FileManager fileManager;
    private PrintStream printStream;
    private Log log = LogFactory.getLog(RDFController.class);
    private static String TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";

    public RDFController(Store store, String remoteModelName, String localModelName) {
        this(store, null, remoteModelName, localModelName, System.out);
    }

    public RDFController(ModelMaker modelMaker, String remoteModelName, String localModelName) {
        this(null, modelMaker, remoteModelName, localModelName, System.out);
    }

    public RDFController(Store store, ModelMaker modelMaker, String remoteModelName, String localModelName, PrintStream printStream) {
        if (store != null) {
            if (remoteModelName != null) {
                this.remoteModelName = remoteModelName;
                this.remoteModel = SDBFactory.connectNamedModel(store, remoteModelName);
            }
            if (localModelName != null) {
                this.localModelName = localModelName;
                this.localModel = SDBFactory.connectNamedModel(store, localModelName);
            }
        } else if (modelMaker != null) {
            if (remoteModelName != null) {
                this.remoteModelName = remoteModelName;
                this.remoteModel = modelMaker.getModel(remoteModelName);
            }
            if (localModelName != null) {
                this.localModelName = localModelName;
                this.localModel = modelMaker.getModel(localModelName);
            }
        } else {
            throw new IllegalArgumentException("Store or ModelMaker must be provided");
        }
        fileManager = FileManager.get();
        this.printStream = printStream;
    }

    public void add(String fileName) throws IOException {
        Model addModel;
        long startTime, endTime, duration;
        long startSize, endSize, sizeDelta;

        log("Adding assertions in " + fileName + " to " + localModelName + " model");
        addModel = loadModelFromFile(fileName);
        startSize = localModel.size();
        startTime = System.currentTimeMillis();
        localModel.add(addModel);
        endTime = System.currentTimeMillis();
        endSize = localModel.size();
        duration = endTime - startTime;
        sizeDelta = endSize - startSize;
        log("Action completed [" + duration + "ms, " + sizeDelta + " records]");

        log("Processing add construct model");
        startTime = System.currentTimeMillis();
        sizeDelta = processAddConstruct(addModel, localModel);
        endTime = System.currentTimeMillis();
        duration = endTime - startTime;
        log("Action completed [" + duration + "ms, " + sizeDelta + " records]");

        log("Processing remove construct model");
        startTime = System.currentTimeMillis();
        sizeDelta = processRemoveConstruct(localModel);
        endTime = System.currentTimeMillis();
        duration = endTime - startTime;
        log("Action completed [" + duration + "ms, " + sizeDelta + " records]");
    }

    public void remove(String fileName) throws IOException {
        long startTime, endTime, duration;
        long startSize, endSize, sizeDelta;
        Model removeModel;

        log("Removing assertions in " + fileName + " from " + localModelName + " model");
        removeModel = loadModelFromFile(fileName);
        startSize = localModel.size();
        startTime = System.currentTimeMillis();
        localModel.remove(removeModel);
        endTime = System.currentTimeMillis();
        duration = endTime - startTime;
        endSize = localModel.size();
        sizeDelta = endSize - startSize;
        log("Action completed [" + duration + "ms, " + sizeDelta + " records]");
    }

    public void process(String addFilename, String delFilename) throws IOException {
        List<QuerySolution> solutionList;
        Model typesModel, outputModel, addModel, delModel;
        long startTime, endTime, duration;
        long startSize, endSize, sizeDelta;

        addModel = loadModelFromFile(addFilename);
        delModel = loadModelFromFile(delFilename);

        log("Adding assertions in " + addFilename + " to " + remoteModelName + " model");
        startSize = remoteModel.size();
        startTime = System.currentTimeMillis();
        remoteModel.add(addModel);
        endTime = System.currentTimeMillis();
        endSize = remoteModel.size();
        duration = endTime - startTime;
        sizeDelta = endSize - startSize;
        log("Action completed [" + duration + "ms, " + sizeDelta + " records]");

        outputModel = ModelFactory.createDefaultModel();

        String typesQuery = "CONSTRUCT { ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?o }\n"
                + "WHERE\n"
                + "{\n"
                + "?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?o\n"
                + "}";
        QueryExecution qe = QueryExecutionFactory.create(typesQuery, Syntax.syntaxARQ, remoteModel);
        typesModel = qe.execConstruct();

        String query = "SELECT DISTINCT ?o WHERE { ?s ?p ?o }";

        solutionList = results(query, localModel);

        for (QuerySolution solution : solutionList) {
            RDFNode node = solution.get("?o");
            if (node.isResource()) {
                String nodeURI = node.asResource().getURI();
                String referredToSubjectQuery = "SELECT ?p ?o WHERE {<" + nodeURI + "> ?p ?o}";
                String referredToObjectQuery = "SELECT ?s ?p WHERE {?s ?p <" + nodeURI + ">}";
                if (resultsSize(referredToSubjectQuery, delModel) > 0 || resultsSize(referredToObjectQuery, delModel) > 0) {
                    /* O is referred to in DEL */
                    if (resultsSize(referredToSubjectQuery, addModel) == 0 && resultsSize(referredToObjectQuery, addModel) == 0) {
                        /* O is not referred to in ADD */
                        String typeAssertionQuery = "SELECT ?type WHERE {<" + nodeURI + "> <" + TYPE + "> ?type}";
                        List<String> typeSolutionList;

                        typeSolutionList = getQueryAttribute(typeAssertionQuery, delModel, "?type");
                        if (!typeSolutionList.isEmpty()) {
                            /* Assertion O <rfd:type> T exists in DEL */
                            List<String> otherTypeSolutionList;

                            otherTypeSolutionList = getQueryAttribute(typeAssertionQuery, typesModel, "?type");
                            if (otherTypeSolutionList.containsAll(typeSolutionList) && otherTypeSolutionList.size() == typeSolutionList.size()) {
                                /* Assertion O <rdf:type> T' (where T' != T) does not exist in TYPES */
                                Model childModel = childModel(node, delModel, null);
                                log("Adding child model for " + nodeURI + " to local model");
                                startSize = localModel.size();
                                startTime = System.currentTimeMillis();
                                localModel.add(childModel);
                                outputModel.add(childModel);
                                endTime = System.currentTimeMillis();
                                endSize = localModel.size();
                                duration = endTime - startTime;
                                sizeDelta = endSize - startSize;
                                log("Action completed [" + duration + "ms, " + sizeDelta + " records]");
                            }
                        }
                    }
                }
            } else if (node.isAnon()) {
                log("Found anon resource: " + node.toString());
            }
        }

        writeModelToFile(outputModel, "children.ttl");
        outputModel.close();

        log("Processing add construct model");
        startTime = System.currentTimeMillis();
        sizeDelta = processAddConstruct(remoteModel, localModel);
        endTime = System.currentTimeMillis();
        duration = endTime - startTime;
        log("Action completed [" + duration + "ms, " + sizeDelta + " records]");
        addModel.close();

        log("Processing remove construct model");
        startTime = System.currentTimeMillis();
        sizeDelta = processRemoveConstruct(remoteModel);
        endTime = System.currentTimeMillis();
        duration = endTime - startTime;
        log("Action completed [" + duration + "ms, " + sizeDelta + " records]");

        log("Deleting assertions in " + delFilename + " from " + remoteModelName + " model");
        startSize = remoteModel.size();
        startTime = System.currentTimeMillis();
        remoteModel.remove(delModel);
        endTime = System.currentTimeMillis();
        endSize = remoteModel.size();
        duration = endTime - startTime;
        sizeDelta = endSize - startSize;
        log("Action completed [" + duration + "ms, " + sizeDelta + " records]");
        delModel.close();
    }

    public void close() {
        if (localModel != null) {
            localModel.close();
        }
        if (remoteModel != null) {
            remoteModel.close();
        }
    }

    private List<String> getQueryAttribute(String query, Model model, String attribute) {
        List<QuerySolution> solutionList;

        solutionList = results(query, model);
        List<String> valueList;

        valueList = new ArrayList<String>(solutionList.size());
        for (QuerySolution typeSolution : solutionList) {
            valueList.add(typeSolution.get(attribute).asResource().getURI());
        }
        return valueList;
    }

    private int resultsSize(String query, Model model) {
        return results(query, model).size();
    }

    private long processAddConstruct(Model sourceModel, Model destinationModel) throws IOException {
        InputStream resourceInputStream;
        StringWriter writer;
        Model constructModel;
        Model totalConstructModel;
        QueryExecution queryExecution;
        long startSize, endSize, sizeDelta;

        startSize = destinationModel.size();
        totalConstructModel = ModelFactory.createDefaultModel();

        for (String addName : getDirectoryContents("/sparql/add")) {
            resourceInputStream = this.getClass().getResourceAsStream(addName);
            writer = new StringWriter();
            IOUtils.copy(resourceInputStream, writer, Charset.defaultCharset());
            queryExecution = QueryExecutionFactory.create(writer.toString(), Syntax.syntaxARQ, sourceModel);
            constructModel = queryExecution.execConstruct();
            destinationModel.add(constructModel);
            writer.close();
            resourceInputStream.close();
            totalConstructModel.add(constructModel);
            constructModel.close();
        }

        writeModelToFile(totalConstructModel, "construct-add.ttl");
        totalConstructModel.close();
        endSize = destinationModel.size();
        sizeDelta = endSize - startSize;
        return sizeDelta;
    }

    private long processRemoveConstruct(Model model) throws IOException {
        InputStream resourceInputStream;
        StringWriter writer;
        Model constructModel;
        Model totalConstructModel;
        QueryExecution queryExecution;
        long startSize, endSize, sizeDelta;

        startSize = model.size();
        totalConstructModel = ModelFactory.createDefaultModel();

        for (String removeName : getDirectoryContents("/sparql/remove")) {
            resourceInputStream = this.getClass().getResourceAsStream(removeName);
            writer = new StringWriter();
            IOUtils.copy(resourceInputStream, writer, Charset.defaultCharset());
            queryExecution = QueryExecutionFactory.create(writer.toString(), Syntax.syntaxARQ, model);
            constructModel = queryExecution.execConstruct();
            model.remove(constructModel);
            writer.close();
            resourceInputStream.close();
            totalConstructModel.add(constructModel);
            constructModel.close();
        }

        writeModelToFile(totalConstructModel, "construct-remove.ttl");
        totalConstructModel.close();

        endSize = model.size();
        sizeDelta = endSize - startSize;
        return sizeDelta;
    }

    private List<String> getDirectoryContents(String directoryName) throws IOException {

        URL directoryUrl = this.getClass().getResource(directoryName);

        if (directoryUrl.getProtocol().equals("jar")) {
            String jarPath;
            JarFile jar;
            Enumeration<JarEntry> entries;
            List<String> result;

            if (directoryName.startsWith("/")) {
                directoryName = directoryName.substring(1);
            }
            jarPath = directoryUrl.getPath().substring(5, directoryUrl.getPath().indexOf("!"));
            jar = new JarFile(URLDecoder.decode(jarPath, "UTF-8"));
            entries = jar.entries();
            result = new ArrayList<String>();
            while (entries.hasMoreElements()) {
                String name;

                name = entries.nextElement().getName();
                if (name.endsWith("/")) {
                    continue;
                }
                if (name.startsWith(directoryName + "/")) {
                    int checkSubdir;

                    checkSubdir = name.indexOf("/");
                    if (checkSubdir >= directoryName.length()) {
                        name = name.substring(0, checkSubdir);
                    }
                    result.add("/" + name);
                }
            }
            return result;
        }
        return Collections.<String>emptyList();
    }

    private static List<QuerySolution> results(String query, Model model) {
        QueryExecution qexec;
        List<QuerySolution> solutionList;
        Iterator<QuerySolution> rs;

        qexec = QueryExecutionFactory.create(query, model);
        rs = qexec.execSelect();
        solutionList = new ArrayList<QuerySolution>();
        for (; rs.hasNext();) {
            solutionList.add(rs.next());
        }
        qexec.close();
        return solutionList;
    }

    private void log(String output) {
        printStream.println(output);
        log.info(output);
    }

    private Model loadModelFromFile(String filename) throws IOException {
        Model fileModel;
        InputStream fileStream;

        fileStream = fileManager.open(filename);
        if (fileStream == null) {
            throw new IOException("Unable to open file: " + filename);
        }
        fileModel = ModelFactory.createDefaultModel();
        fileModel.read(fileStream, "", "N-TRIPLE");
        try {
            fileStream.close();
        } catch (IOException ioe) {
            log("Error closing stream: " + ioe.getMessage());
        }
        return fileModel;
    }

    private void writeModelToFile(Model model, String fileName) throws IOException {
        FileOutputStream fos;

        fos = new FileOutputStream(fileName);
        model.write(fos, "N-TRIPLE");
        fos.close();
    }

    private static Model childModel(RDFNode subjectNode, Model originModel, Model childModel) {
        if (childModel == null) {
            childModel = ModelFactory.createDefaultModel();
        }

        Resource subject = subjectNode.asResource();
        String subjectURI = subject.getURI();
        String referredToSubjectQuery = "SELECT ?p ?o WHERE {<" + subjectURI + "> ?p ?o}";

        for (QuerySolution solution : results(referredToSubjectQuery, originModel)) {
            RDFNode propertyNode;
            RDFNode objectNode;
            Property property;

            propertyNode = solution.get("?p");
            property = childModel.createProperty(propertyNode.asResource().getURI());
            objectNode = solution.get("?o");
            childModel.add(subject, property, objectNode);
            if (objectNode.isResource()) {
                if (childModel.contains(objectNode.asResource(), null)) {
                    continue;
                }
                childModel = childModel(objectNode, originModel, childModel);
            }
        }
        return childModel;
    }
}
