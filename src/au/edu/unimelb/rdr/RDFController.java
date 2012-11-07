package au.edu.unimelb.rdr;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.ModelMaker;
import com.hp.hpl.jena.sdb.SDBFactory;
import com.hp.hpl.jena.sdb.Store;
import com.hp.hpl.jena.util.FileManager;
import java.io.IOException;
import java.io.InputStream;

public class RDFController {

    private Model model;
    private FileManager fileManager;

    public RDFController(Store store, String modelName) {
        fileManager = FileManager.get();
        model = SDBFactory.connectNamedModel(store, modelName);
    }

    public RDFController(ModelMaker mdlMaker, String modelName) {
        model = mdlMaker.getModel(modelName);
    }

    public void add(String fileName) throws IOException {
        InputStream is = fileManager.open(fileName);
        System.out.println("Adding RDF to database...");
        model.read(is, "", "N-TRIPLE");
        is.close();
    }

    public void remove(String fileName) throws IOException {
        InputStream is = fileManager.open(fileName);
        System.out.println("Removing RDF from database...");
        Model removeModel = ModelFactory.createDefaultModel();
        removeModel.read(is, "", "N-TRIPLE");
        model.remove(removeModel);
        is.close();
    }
}