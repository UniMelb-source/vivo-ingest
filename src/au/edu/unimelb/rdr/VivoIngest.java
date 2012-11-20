package au.edu.unimelb.rdr;

import au.edu.unimelb.rdr.database.DatabaseConnection;
import au.edu.unimelb.rdr.database.SDBDatabaseConnection;
import com.hp.hpl.jena.ontology.impl.OntModelImpl;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.Syntax;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.sdb.SDBFactory;
import com.hp.hpl.jena.sdb.Store;
import java.io.IOException;
import java.util.Iterator;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class VivoIngest {

    private static Log log = LogFactory.getLog(RDFController.class);

    private static CommandLine parseOptions(String[] args) throws ParseException {
        Options options = new Options();
        options.addOption(OptionBuilder.hasArg(true).isRequired(true).withDescription("Database user name").create("userName"));
        options.addOption(OptionBuilder.hasArg(true).isRequired(true).withDescription("Database password").create("password"));
        options.addOption(OptionBuilder.hasArg(true).isRequired(true).withDescription("Database string").create("dbString"));
        options.addOption(OptionBuilder.hasArg(true).isRequired(true).withDescription("Remote model name").create("remoteModelName"));
        options.addOption(OptionBuilder.hasArg(true).isRequired(true).withDescription("Local model name").create("localModelName"));
        options.addOption(OptionBuilder.hasArg(true).isRequired(false).withDescription("Add filename").create("addFileName"));
        options.addOption(OptionBuilder.hasArg(true).isRequired(false).withDescription("Delete filename").create("delFileName"));
        options.addOption(OptionBuilder.hasArg(true).isRequired(false).withDescription("Database type").create("dbType"));
        options.addOption(OptionBuilder.hasArg(false).isRequired(false).withDescription("Show help").create("h"));
        CommandLineParser parser = new BasicParser();
        CommandLine cmd = parser.parse(options, args);
        if (cmd.hasOption("h")) {
            consoleHelp();
        }
        return cmd;
    }

    public static void main(String[] args) throws IOException {
        CommandLine cmd = null;
        try {
            cmd = parseOptions(args);
        } catch (ParseException pe) {
            System.out.println(pe.getMessage());
            consoleHelp();
            return;
        }

        String dbType = null;

        if (cmd.hasOption("dbType")) {
            dbType = cmd.getOptionValue("dbType");
        } else {
            dbType = "MySQL";
        }

        String remoteModelName = cmd.getOptionValue("remoteModelName");
        String localModelName = cmd.getOptionValue("localModelName");
        String password = cmd.getOptionValue("password");
        String dbString = cmd.getOptionValue("dbString");
        String addFileName = cmd.getOptionValue("addFileName");
        String delFileName = cmd.getOptionValue("delFileName");
        String userName = cmd.getOptionValue("userName");

        RDFController controller;
        SDBDatabaseConnection sdc = new SDBDatabaseConnection(userName, password, dbString, dbType);
        Store store = sdc.getStore();
        controller = new RDFController(store, remoteModelName, localModelName);
        controller.process(addFileName, delFileName);
        controller.close();
    }

    /*public static void main(String[] args) {
     CommandLine cmd = null;
     try {
     cmd = parseOptions(args);
     } catch (ParseException pe) {
     System.out.println(pe.getMessage());
     consoleHelp();
     }

     String jenaType = null;
     String dbType = null;

     Boolean addData = null;

     String modelName = null;

     Boolean dropTables = null;

     if (cmd.hasOption("jenaType")) {
     jenaType = cmd.getOptionValue("jenaType");
     } else {
     jenaType = "SDB";
     }

     if (cmd.hasOption("dbType")) {
     dbType = cmd.getOptionValue("dbType");
     } else {
     dbType = "MySQL";
     }

     if (cmd.hasOption("addData")) {
     addData = Boolean.parseBoolean(cmd.getOptionValue("addData"));
     } else {
     addData = Boolean.TRUE;
     }

     if (cmd.hasOption("modelName")) {
     modelName = cmd.getOptionValue("modelName");
     } else {
     modelName = "http://vitro.mannlib.cornell.edu/default/vitro-kb-2";
     }

     if (cmd.hasOption("dropTables")) {
     dropTables = Boolean.parseBoolean(cmd.getOptionValue("dropTables"));
     } else {
     dropTables = Boolean.FALSE;
     }

     String password = cmd.getOptionValue("password");
     String dbString = cmd.getOptionValue("dbString");
     String fileName = cmd.getOptionValue("fileName");
     String userName = cmd.getOptionValue("userName");

     if (dropTables) {
     Connection conn = null;

     if (jenaType.equalsIgnoreCase("SDB")) {
     SDBDatabaseConnection sdc = new SDBDatabaseConnection(userName, password, dbString, dbType);
     conn = sdc.getConnection();
     } else {
     DatabaseConnection dbc = new DatabaseConnection(userName, password, dbString, dbType);
     try {
     conn = dbc.getConnection();
     } catch (SQLException sqle) {
     error("Cannot create database connection: " + sqle.toString());
     return;
     }
     }
     try {
     if (dbType.equalsIgnoreCase("Oracle")) {
     String query = "BEGIN FOR i IN (SELECT table_name FROM user_tables) LOOP EXECUTE IMMEDIATE('DROP TABLE ' || user || '.' || i.table_name || ' CASCADE CONSTRAINTS'); END LOOP; END;";

     CallableStatement callStmt = conn.prepareCall(query);
     callStmt.execute();
     } else {
     String query = "SELECT CONCAT( \"DROP TABLE \",GROUP_CONCAT(TABLE_NAME)) AS stmt FROM information_schema.TABLES";

     Statement statement = conn.createStatement();
     statement.executeQuery(query);
     }
     } catch (SQLException sqle) {
     error("Cannot drop tables: " + sqle.toString());
     return;
     }

     } else {
     RDFController controller;
     if (jenaType.equals("SDB")) {
     SDBDatabaseConnection sdc = new SDBDatabaseConnection(userName, password, dbString, dbType);
     Store store = sdc.getStore();
     controller = new RDFController(store, modelName);
     } else {
     DatabaseConnection dbc = new DatabaseConnection(userName, password, dbString, dbType);
     controller = new RDFController(dbc.maker, modelName);
     }
     try {
     if (addData) {
     controller.add(fileName);
     } else {
     controller.remove(fileName);
     }
     } catch (IOException ioe) {
     error("Can't perform RDF operation: " + ioe.getMessage());
     return;
     }
     }
     }*/
    private static void consoleHelp() {
        System.out.print("Correct usage of this application: ");
        System.out.println("java -jar VivoIngest.jar <flags>\n");
        System.out.println("If a database type isn't chosen, this program will use MySQL as the default.");
        System.out.println("\n\tFlags:");
        System.out.println("\n\t-userName johnDoe");
        System.out.println("\t-password secret123");
        System.out.println("\t-dbString jdbc:mysql://localhost/vivo");
        System.out.println("\t-dbType MySQL/Oracle");
        System.out.println("\t-addFileName add.ttl");
        System.out.println("\t-addFileName del.ttl");
        System.out.println("\t-remoteModelName remote-model");
        System.out.println("\t-localModelName local-model");
        System.exit(0);
    }

    private static void info(String output) {
        System.out.println(output);
        log.info(output);
    }

    private static void error(String output) {
        System.out.println(output);
        log.error(output);
    }
}
