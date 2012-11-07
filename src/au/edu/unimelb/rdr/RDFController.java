package au.edu.unimelb.rdr;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.ModelMaker;
import com.hp.hpl.jena.sdb.SDBFactory;
import com.hp.hpl.jena.sdb.Store;
import com.hp.hpl.jena.util.FileManager;
import java.io.InputStream;

public class RDFController {

    public RDFController(Store store, String fileName, String modelName, Boolean addRDF)
            throws Exception {
        Model sdbModel = SDBFactory.connectNamedModel(store, modelName);
        InputStream is = FileManager.get().open(fileName);

        if (addRDF.booleanValue()) {
            System.out.println("Adding RDF to database...");
            sdbModel.read(is, "", "N-TRIPLE");
            is.close();
        } else {
            System.out.println("Removing RDF from database...");
            Model removeModel = ModelFactory.createDefaultModel();
            removeModel.read(is, "", "N-TRIPLE");
            sdbModel.remove(removeModel);
            is.close();
        }
    }

    public RDFController(ModelMaker mdlMaker, String fileName, String modelName, Boolean addRDF) throws Exception {
        Model m = mdlMaker.getModel(modelName);

        InputStream is = FileManager.get().open(fileName);

        if (addRDF.booleanValue()) {
            System.out.println("Adding RDF to database...");
            m.read(is, "", "N-TRIPLE");
            is.close();
        } else {
            System.out.println("Removing RDF from database...");
            Model removeModel = ModelFactory.createDefaultModel();
            removeModel.read(is, "", "N-TRIPLE");
            m.remove(removeModel);
            is.close();
        }
    }
}