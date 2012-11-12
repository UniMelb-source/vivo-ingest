package au.edu.unimelb.rdr;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.ModelMaker;
import com.hp.hpl.jena.sdb.SDBFactory;
import com.hp.hpl.jena.sdb.Store;
import com.hp.hpl.jena.util.FileManager;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class RDFController {

    private Model model;
    private FileManager fileManager;
    private PrintStream printStream;
    private Log log = LogFactory.getLog(RDFController.class);

    public RDFController(Store store, String modelName) {
        this(store, modelName, System.out);
    }

    public RDFController(Store store, String modelName, PrintStream printStream) {
        fileManager = FileManager.get();
        model = SDBFactory.connectNamedModel(store, modelName);
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
    
    private void log(String output) {
        printStream.println(output);
        log.info(output);
    }
}