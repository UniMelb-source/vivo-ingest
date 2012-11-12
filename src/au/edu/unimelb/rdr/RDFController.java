package au.edu.unimelb.rdr;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.ModelMaker;
import com.hp.hpl.jena.sdb.SDBFactory;
import com.hp.hpl.jena.sdb.Store;
import com.hp.hpl.jena.util.FileManager;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;

public class RDFController {

    private Model model;
    private FileManager fileManager;
    private PrintStream printStream;

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
        printStream.println("Adding RDF to database...");
        model.read(inputStream, "", "N-TRIPLE");
        inputStream.close();
        printStream.println("Action completed.");
    }

    public void remove(String fileName) throws IOException {
        InputStream inputStream = fileManager.open(fileName);
        printStream.println("Removing RDF from database...");
        Model removeModel = ModelFactory.createDefaultModel();
        removeModel.read(inputStream, "", "N-TRIPLE");
        model.remove(removeModel);
        inputStream.close();
        printStream.println("Action completed.");
    }
}