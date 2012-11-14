package au.edu.unimelb.rdr;

import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.Syntax;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.ModelMaker;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
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

    private Model model;
    private FileManager fileManager;
    private PrintStream printStream;
    private Store store;
    private Log log = LogFactory.getLog(RDFController.class);
    private static String UPSTREAM_MODEL = "";
    private static String KEEP_MODEL = "";
    private static String VIVO_MODEL = "http://vitro.mannlib.cornell.edu/default/vitro-kb-2";
    private static String TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";

    public RDFController(Store store, String modelName) {
        this(store, modelName, System.out);
    }

    public RDFController(Store store, String modelName, PrintStream printStream) {
        fileManager = FileManager.get();
        model = SDBFactory.connectNamedModel(store, modelName);
        this.store = store;
        this.printStream = printStream;
    }

    public RDFController(ModelMaker modelMaker, String modelName) {
        this(modelMaker, modelName, System.out);
    }

    public RDFController(ModelMaker modelMaker, String modelName, PrintStream printStream) {
        model = modelMaker.getModel(modelName);
        this.printStream = printStream;
    }

    public void add(String fileName) throws IOException {
        InputStream inputStream = fileManager.open(fileName);
        log("Adding RDF to database...");
        long startTime = System.currentTimeMillis();
        long startSize = model.size();
        model.read(inputStream, "", "N-TRIPLE");
        inputStream.close();
        long endTime = System.currentTimeMillis();
        long endSize = model.size();
        long duration = endTime - startTime;
        long sizeDelta = endSize - startSize;
        log("Action completed [" + duration + "ms, " + sizeDelta + " records]");
    }

    public void remove(String fileName) throws IOException {
        InputStream inputStream = fileManager.open(fileName);
        log("Removing RDF from database...");
        long startTime = System.currentTimeMillis();
        long startSize = model.size();
        Model removeModel = ModelFactory.createDefaultModel();
        removeModel.read(inputStream, "", "N-TRIPLE");
        model.remove(removeModel);
        inputStream.close();
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        long endSize = model.size();
        long sizeDelta = endSize - startSize;
        log("Action completed [" + duration + "ms, " + sizeDelta + " records]");
    }

    public void process(String addFilename, String delFilename) {
        InputStream addStream = fileManager.open(addFilename);
        InputStream delStream = fileManager.open(delFilename);

        Model addModel = ModelFactory.createDefaultModel();
        addModel.read(addStream, "", "N-TRIPLE");
        addStream.close();

        Model delModel = ModelFactory.createDefaultModel();
        delModel.read(delStream, "", "N-TRIPLE");
        delStream.close();

        String typesQuery = "CONSTRUCT ?s ?p ?o"
                + "WHERE"
                + "{"
                + "BIND(" + TYPE + " as ?p)"
                + "?s ?p ?o"
                + "}";

        Model typesModel = QueryExecutionFactory.create(typesQuery, faeModel).execConstruct();



        /*Step 1:
         Add ADD to FAE*/

        model.add(addModel);

        /*Step 2:
         For each assertion (S, P, O) in RDR:
         * */
        String query = "SELECT DISTINCT ?o WHERE { ?s ?p ?o }";
        List<QuerySolution> solutionList;

        solutionList = runQuery(query, model);

        for (QuerySolution solution : solutionList) {
            RDFNode node = solution.get("?o");
            if (node.isResource()) {
                String nodeURI = node.asResource().getURI();
                String referredToSubjectQuery = "SELECT ?p ?o WHERE {" + nodeURI + " ?p ?o}";
                String referredToObjectQuery = "SELECT ?s ?p WHERE {?s ?p " + nodeURI + "}";
                if (runQuerySize(referredToSubjectQuery, addModel) == 0) {
                    if (runQuerySize(referredToObjectQuery, addModel) == 0) {
                        /*O is not referred to in ADD*/
                        if (runQuerySize(referredToSubjectQuery, delModel) > 0
                                || runQuerySize(referredToObjectQuery, delModel) > 0) {
                            /*O is referred to in DEL*/
                            String typeAssertionQuery = "SELECT ?type WHERE {" + nodeURI + " " + TYPE + " ?type}";
                            List<String> typeSolutionList;

                            typeSolutionList = getQueryAttribute(typeAssertionQuery, delModel, "?type");
                            if (!typeSolutionList.isEmpty()) {

                                /*Assertion O <rfd:type> T exists in DEL */
                                List<String> otherTypeSolutionList;

                                otherTypeSolutionList = getQueryAttribute(typeAssertionQuery, typesModel, "?type");
                                if (otherTypeSolutionList.containsAll(typeSolutionList) && otherTypeSolutionList.size() == typeSolutionList.size()) {
                                    /* Assertion O <rdf:type> T' (where T' != T) does not exist in TYPES */
                                }
                            }
                        }
                    }
                }
            }
            /*
             If

             consider this a deletion, determine children and add this into RDR
             Else
             consider this an edit of a property/subproperty and continue*/
            System.out.println(solution.toString());

        }



        /*Step 3:
         Delete DEL from FAE*/
        model.remove(delModel);


        log("Adding RDF to database...");

    }

    private List<String> getQueryAttribute(String query, Model model, String attribute) {
        List<QuerySolution> solutionList;

        solutionList = runQuery(query, model);
        List<String> valueList;

        valueList = new ArrayList<String>(solutionList.size());
        for (QuerySolution typeSolution : solutionList) {
            valueList.add(typeSolution.get(attribute).asResource().getURI());
        }
        return valueList;
    }

    private int runQuerySize(String query, Model model) {
        return runQuery(query, model).size();
    }

    private List<QuerySolution> runQuery(String query, Model model) {
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

    public void pruneModel(Model upstreamModel) {
        Model vivoModel = SDBFactory.connectNamedModel(store, VIVO_MODEL);
        Model keepModel = SDBFactory.connectNamedModel(store, KEEP_MODEL);
        String query = "SELECT DISTINCT ?s WHERE { ?s ?p ?o }";
        QueryExecution qexec = QueryExecutionFactory.create(query, upstreamModel);
        try {

            Iterator<QuerySolution> rs = qexec.execSelect();
            for (; rs.hasNext();) {
                QuerySolution soln = rs.next();
                System.out.println(soln.toString());

            }
        } finally {
            qexec.close();
        }
    }

    private void log(String output) {
        printStream.println(output);
        log.info(output);
    }
}
