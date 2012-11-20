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
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class RDFController {

    private Model remoteModel;
    private Model localModel;
    private FileManager fileManager;
    private PrintStream printStream;
    private Log log = LogFactory.getLog(RDFController.class);
    //"http://vitro.mannlib.cornell.edu/default/vitro-kb-2";
    private static String TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";

    public RDFController(Store store, String remoteModelName, String localModelName) {
        this(store, null, remoteModelName, localModelName, System.out);
    }

    public RDFController(ModelMaker modelMaker, String remoteModelName, String localModelName) {
        this(null, modelMaker, remoteModelName, localModelName, System.out);
    }

    public RDFController(Store store, ModelMaker modelMaker, String remoteModelName, String localModelName, PrintStream printStream) {
        if (store != null) {
            this.remoteModel = SDBFactory.connectNamedModel(store, remoteModelName);
            this.localModel = SDBFactory.connectNamedModel(store, localModelName);
        } else if (modelMaker != null) {
            this.remoteModel = modelMaker.getModel(remoteModelName);
            this.localModel = modelMaker.getModel(localModelName);
        } else {
            throw new IllegalArgumentException("Store or ModelMaker must be provided");
        }
        this.fileManager = FileManager.get();
        this.printStream = printStream;
    }

    public void add(String fileName) throws IOException {
        InputStream inputStream = fileManager.open(fileName);
        log("Adding RDF to database...");
        long startTime = System.currentTimeMillis();
        long startSize = remoteModel.size();
        remoteModel.read(inputStream, "", "N-TRIPLE");
        inputStream.close();
        long endTime = System.currentTimeMillis();
        long endSize = remoteModel.size();
        long duration = endTime - startTime;
        long sizeDelta = endSize - startSize;
        log("Action completed [" + duration + "ms, " + sizeDelta + " records]");
    }

    public void remove(String fileName) throws IOException {
        InputStream inputStream = fileManager.open(fileName);
        log("Removing RDF from database...");
        long startTime = System.currentTimeMillis();
        long startSize = remoteModel.size();
        Model removeModel = ModelFactory.createDefaultModel();
        removeModel.read(inputStream, "", "N-TRIPLE");
        remoteModel.remove(removeModel);
        inputStream.close();
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        long endSize = remoteModel.size();
        long sizeDelta = endSize - startSize;
        log("Action completed [" + duration + "ms, " + sizeDelta + " records]");
    }

    public void process(String addFilename, String delFilename) throws IOException {
        /* Temporary models used in processing */
        Model addModel = null;
        Model delModel = null;

        if (addFilename != null) {
            addModel = loadModelFromFile(addFilename);
        }
        if (delFilename != null) {
            delModel = loadModelFromFile(delFilename);
        }

        if (addModel != null) {
            List<QuerySolution> solutionList;
            Model typesModel;

            log("Adding " + addFilename + " to remote model");
            remoteModel.add(addModel);

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
                                    log("Adding child model for " + nodeURI + " to local model, " + childModel.size() + " assertions");
                                    localModel.add(childModel);
                                }
                            }
                        }
                    }
                } else if (node.isAnon()) {
                    log("Found anon resource: " + node.toString());
                }
            }
        }

        if (delModel != null) {
            log("Deleting " + delFilename + " from remote model");
            remoteModel.remove(delModel);
        }

        addModel.close();
        delModel.close();
    }

    public void close() {
        this.localModel.close();
        this.remoteModel.close();
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

    private List<QuerySolution> results(String query, Model model) {
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

    private Model childModel(RDFNode subjectNode, Model originModel, Model childModel) {
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
                    /* The object of this result has already been processed as a
                     *  subject so continue
                     */
                    continue;
                }
                childModel = childModel(objectNode, originModel, childModel);
            }
        }
        return childModel;
    }
}
