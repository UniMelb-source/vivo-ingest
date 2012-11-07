package au.edu.unimelb.rdr;

import au.edu.unimelb.rdr.database.DatabaseConnection;
import au.edu.unimelb.rdr.database.SDBDatabaseConnection;
import com.hp.hpl.jena.sdb.Store;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Statement;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class VivoIngest {

    private static CommandLine parseOptions(String[] args) throws ParseException {
        Options options = new Options();
        options.addOption(OptionBuilder.hasArg(true).isRequired(true).withDescription("Database user name").create("userName"));
        options.addOption(OptionBuilder.hasArg(true).isRequired(true).withDescription("Database password").create("password"));
        options.addOption(OptionBuilder.hasArg(true).isRequired(true).withDescription("Database string").create("dbString"));
        options.addOption(OptionBuilder.hasArg(true).isRequired(false).withDescription("TTL filename").create("fileName"));
        options.addOption(OptionBuilder.hasArg(true).isRequired(false).withDescription("JENA type").create("jenaType"));
        options.addOption(OptionBuilder.hasArg(true).isRequired(false).withDescription("Database type").create("dbType"));
        options.addOption(OptionBuilder.hasArg(true).isRequired(false).withDescription("Add data?").create("addData"));
        options.addOption(OptionBuilder.hasArg(true).isRequired(false).withDescription("Model name").create("modelName"));
        options.addOption(OptionBuilder.hasArg(true).isRequired(false).withDescription("Drop tables?").create("dropTables"));
        options.addOption(OptionBuilder.hasArg(false).isRequired(false).withDescription("Show help").create("h"));
        CommandLineParser parser = new BasicParser();
        CommandLine cmd = parser.parse(options, args);
        if (cmd.hasOption("h")) {
            consoleHelp();
        }
        if (!cmd.hasOption("dropTables")) {
            System.out.println("Missing required option: fileName");
            consoleHelp();
        }
        return cmd;
    }

    public static void main(String[] args) throws Exception {
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
                conn = dbc.getConnection();
            }

            if (dbType.equalsIgnoreCase("Oracle")) {
                String query = "BEGIN FOR i IN (SELECT table_name FROM user_tables) LOOP EXECUTE IMMEDIATE('DROP TABLE ' || user || '.' || i.table_name || ' CASCADE CONSTRAINTS'); END LOOP; END;";

                CallableStatement callStmt = conn.prepareCall(query);
                callStmt.execute();
            } else {
                String query = "SELECT CONCAT( \"DROP TABLE \",GROUP_CONCAT(TABLE_NAME)) AS stmt FROM information_schema.TABLES";

                Statement statement = conn.createStatement();
                statement.executeQuery(query);
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
            if (addData) {
                controller.add(fileName);
            } else {
                controller.remove(fileName);
            }
        }
    }

    private static void consoleHelp() {
        System.out.print("Correct usage of this application: ");
        System.out.println("java -jar VivoIngest.jar <flags>\n");
        System.out.println("If a Jena type isn't chosen, this program will use SDB as the default.");
        System.out.println("If a database type isn't chosen, this program will use MySQL as the default.");
        System.out.println("If adding or removing data isn't specified, this program will use adding data as the default.");
        System.out.println("If a model name isn't chosen, this program will use kb-2 as default (the main VIVO model).\n");
        System.out.println("If -dropTables is used, only the username, password and dbString are required (And optionally, the jenaType and dbType).\n");
        System.out.println("\n\tFlags:");
        System.out.println("\n\t-jenaType SDB/RDB");
        System.out.println("\t-userName johnDoe");
        System.out.println("\t-password secret123");
        System.out.println("\t-dbString jdbc:mysql://localhost/vivo");
        System.out.println("\t-dbType MySQL/Oracle");
        System.out.println("\t-fileName bigfile6");
        System.out.println("\t-addData true/false");
        System.out.println("\t-modelName MODEL1");
        System.out.println("\t-dropTables");
        System.exit(0);
    }
}
