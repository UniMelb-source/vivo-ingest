package au.edu.unimelb.rdr;

import com.hp.hpl.jena.sdb.Store;
import au.edu.unimelb.rdr.database.DatabaseConnection;
import au.edu.unimelb.rdr.database.SDBDatabaseConnection;
import java.io.PrintStream;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Statement;
import org.apache.commons.lang.StringUtils;

public class VivoIngest {

    public static void main(String[] args)
            throws Exception {
        String jenaType = "SDB";
        String dbType = "MySQL";
        String modelName = "http://vitro.mannlib.cornell.edu/default/vitro-kb-2";

        String password = "";
        String dbString = "";
        String fileName = "";
        String userName = "";
        String addData = "";

        Boolean dropTables = Boolean.valueOf(false);
        Boolean addRDF = Boolean.valueOf(true);

        int i = 0;

        for (String s : args) {
            if (s.equalsIgnoreCase("-jenaType")) {
                jenaType = args[(i + 1)];
            } else if (s.equalsIgnoreCase("-userName")) {
                userName = args[(i + 1)];
            } else if (s.equalsIgnoreCase("-password")) {
                password = args[(i + 1)];
            } else if (s.equalsIgnoreCase("-dbType")) {
                dbType = args[(i + 1)];
            } else if (s.equalsIgnoreCase("-dbString")) {
                dbString = args[(i + 1)];
                dbString = StringUtils.strip(dbString, "\"");
            } else if (s.equalsIgnoreCase("-fileName")) {
                fileName = args[(i + 1)];
            } else if (s.equalsIgnoreCase("-addData")) {
                addData = args[(i + 1)];
                addRDF = Boolean.valueOf(Boolean.parseBoolean(addData));
            } else if (s.equalsIgnoreCase("-modelName")) {
                modelName = args[(i + 1)];
            } else if (s.equalsIgnoreCase("-dropTables")) {
                dropTables = Boolean.valueOf(true);
            } else if (s.equalsIgnoreCase("-h")) {
                consoleHelp();
            }

            i++;
        }
        if (dropTables.booleanValue()) {
            if ((userName.equalsIgnoreCase("")) || (password.equalsIgnoreCase("")) || (dbString.equalsIgnoreCase(""))) {
                System.out.println("Login credentials and dbString are required for the drop tables command.\n");
                consoleHelp();
                System.exit(0);
            }

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
            if ((userName.equalsIgnoreCase("")) || (password.equalsIgnoreCase("")) || (dbString.equalsIgnoreCase(""))) {
                System.out.println("This program needs a user name, a password, a database string, and a filename to run.\n");
                consoleHelp();
                System.exit(0);
            }
            RDFController rdfC;
            if (jenaType.equals("SDB")) {
                SDBDatabaseConnection sdc = new SDBDatabaseConnection(userName, password, dbString, dbType);
                Store store = sdc.getStore();
                rdfC = new RDFController(store, fileName, modelName, addRDF);
            } else {
                DatabaseConnection dbc = new DatabaseConnection(userName, password, dbString, dbType);
                rdfC = new RDFController(dbc.maker, fileName, modelName, addRDF);
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
        System.out.println("\t-h - Display this help menu\n");
        System.exit(0);
    }
}
