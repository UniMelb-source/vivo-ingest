package au.edu.unimelb.rdr;

import au.edu.unimelb.rdr.database.DatabaseConnection;
import au.edu.unimelb.rdr.database.SDBDatabaseConnection;
import com.hp.hpl.jena.sdb.Store;
import java.io.IOException;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class VivoIngest {

    private static Log log = LogFactory.getLog(RDFController.class);

    private static CommandLine parseOptions(String[] args) {
        Options options;
        CommandLineParser parser;
        CommandLine cmd = null;

        options = new Options();
        options.addOption(OptionBuilder.withArgName("database-user-name").hasArg().isRequired(true).withDescription("Database user name").create("userName"));
        options.addOption(OptionBuilder.withArgName("database-password").hasArg().isRequired(true).withDescription("Database password").create("password"));
        options.addOption(OptionBuilder.withArgName("database-connection-string").hasArg().isRequired(true).withDescription("Database string").create("dbString"));
        options.addOption(OptionBuilder.withArgName("remote-model").hasArg().isRequired(true).withDescription("Remote model name").create("remoteModelName"));
        options.addOption(OptionBuilder.withArgName("local-model").hasArg().isRequired(true).withDescription("Local model name").create("localModelName"));
        options.addOption(OptionBuilder.withArgName("jena-database-type").hasArg().isRequired(true).withDescription("JENA database type").create("jenaType"));
        options.addOption(OptionBuilder.withArgName("add-delta-ttl").hasArg().isRequired(false).withDescription("Add filename").create("addFileName"));
        options.addOption(OptionBuilder.withArgName("delete-delta-ttl").hasArg().isRequired(false).withDescription("Delete filename").create("delFileName"));
        options.addOption(OptionBuilder.withArgName("database-type").hasArg().isRequired(false).withDescription("Database type").create("dbType"));
        options.addOption(OptionBuilder.hasArg(false).isRequired(false).withDescription("Show help").create("h"));
        parser = new BasicParser();
        try {
            cmd = parser.parse(options, args);
            if (cmd.hasOption("h")) {
                consoleHelp(options);
            }

        } catch (ParseException pe) {
            consoleHelp(options);
        }
        return cmd;
    }

    public static void main(String[] args) throws IOException {
        CommandLine cmd = null;
        SDBDatabaseConnection sdbConnection = null;
        DatabaseConnection rdbConnection = null;
        cmd = parseOptions(args);

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
        String jenaType = cmd.getOptionValue("jenaType");

        RDFController controller;
        if (jenaType.equals("SDB")) {
            sdbConnection = new SDBDatabaseConnection(userName, password, dbString, dbType);
            Store store = sdbConnection.getStore();
            controller = new RDFController(store, remoteModelName, localModelName);
        } else {
            rdbConnection = new DatabaseConnection(userName, password, dbString, dbType);
            controller = new RDFController(rdbConnection.maker, remoteModelName, localModelName);
        }
        controller.process(addFileName, delFileName);
        controller.close();
        if (sdbConnection != null) {
            sdbConnection.closeDatabaseConnection();
        }
        if (rdbConnection != null) {
            rdbConnection.closeDatabaseConnection();
        }
    }

    private static void consoleHelp(Options options) {
        HelpFormatter formatter = new HelpFormatter();

        formatter.printHelp("java -jar VivoIngest.jar", options);
        System.exit(1);
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
