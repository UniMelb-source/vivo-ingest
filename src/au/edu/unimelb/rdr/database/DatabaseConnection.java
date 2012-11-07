package au.edu.unimelb.rdr.database;

import com.hp.hpl.jena.db.DBConnection;
import com.hp.hpl.jena.db.IDBConnection;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.ModelMaker;
import java.sql.Connection;
import java.sql.SQLException;

public class DatabaseConnection {

    public ModelMaker maker;
    public IDBConnection conn;

    public DatabaseConnection(String userName, String password, String dbString, String dbType) {
        String className = "";

        if (dbType.equalsIgnoreCase("MySQL")) {
            className = "com.mysql.jdbc.Driver";
        } else {
            className = "oracle.jdbc.driver.OracleDriver";
        }

        try {
            Class.forName(className);

            this.conn = new DBConnection(dbString, userName, password, dbType);
        } catch (Exception e) {
            System.out.println(e);
        }

        this.maker = ModelFactory.createModelRDBMaker(this.conn);
    }

    public ModelMaker getModelMaker() {
        return this.maker;
    }

    public Connection getConnection() throws SQLException {
        return this.conn.getConnection();
    }

    public void closeDatabaseConnection() {
        try {
            this.maker.close();
            this.conn.close();
        } catch (Exception e) {
            System.out.println(e);
        }
    }
}