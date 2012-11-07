package au.edu.unimelb.rdr.database;

import com.hp.hpl.jena.rdf.model.ModelMaker;
import com.hp.hpl.jena.sdb.SDBFactory;
import com.hp.hpl.jena.sdb.Store;
import com.hp.hpl.jena.sdb.StoreDesc;
import com.hp.hpl.jena.sdb.sql.SDBConnection;
import com.hp.hpl.jena.sdb.store.DatabaseType;
import com.hp.hpl.jena.sdb.store.LayoutType;
import java.sql.Connection;
import org.apache.commons.dbcp.BasicDataSource;

public class SDBDatabaseConnection {

    public ModelMaker maker;
    public SDBConnection conn;
    public Store store;

    public SDBDatabaseConnection(String userName, String password, String dbString, String dbType) {
        String className = "";

        if (dbType.equalsIgnoreCase("MySQL")) {
            className = "com.mysql.jdbc.Driver";
        } else {
            className = "oracle.jdbc.driver.OracleDriver";
        }

        try {
            StoreDesc storeDesc = new StoreDesc(LayoutType.fetch("layout2/hash"), DatabaseType.fetch(dbType));

            BasicDataSource ds = new BasicDataSource();
            ds.setDriverClassName(className);
            ds.setUrl(dbString);
            ds.setUsername(userName);
            ds.setPassword(password);

            this.conn = new SDBConnection(ds.getConnection());

            this.store = SDBFactory.connectStore(this.conn, storeDesc);
            if (this.store == null) {
                System.out.println("Store is null...");
            }
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    public Connection getConnection() {
        return this.conn.getSqlConnection();
    }

    public Store getStore() {
        return this.store;
    }

    public void closeDatabaseConnection() {
        try {
            this.store.close();
            this.conn.close();
        } catch (Exception e) {
            System.out.println(e);
        }
    }
}