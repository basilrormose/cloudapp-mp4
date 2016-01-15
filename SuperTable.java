import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;

import org.apache.hadoop.hbase.TableName;

import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import org.apache.hadoop.hbase.util.Bytes;

public class SuperTable {

    public static void main(String[] args) throws IOException {
        
        final String TABLE_NAME = "powers";
        final String CF_PERSONAL = "personal";
        final String CF_PROFESSIONAL = "professional";

        final String COL_HERO = "hero";
        final String COL_POWER = "power";
        final String COL_NAME = "name";
        final String COL_XP = "xp";

        try {
            // Instantiate Configuration class
            //Configuration con = HBaseConfiguration.create();

            Connection conn = ConnectionFactory.createConnection();
            Admin admin = conn.getAdmin();

            // Instantiate HBaseAdmin class
            //HBaseAdmin admin = new HBaseAdmin(con);

            TableName tableName = TableName.valueOf(TABLE_NAME);
            if (admin.tableExists(tableName))
            {
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
            }

            // Instantiate table descriptor class
            HTableDescriptor table = new HTableDescriptor(TableName.valueOf(TABLE_NAME));

            // Add column families to table descriptor
            table.addFamily(new HColumnDescriptor(CF_PERSONAL));
            table.addFamily(new HColumnDescriptor(CF_PROFESSIONAL));

            // Execute the table through admin
            admin.createTable(table);

            // Instantiating HTable class
            Table powers = conn.getTable(tableName);

            // Repeat these steps as many times as necessary
            // Instantiating Put class
            // Hint: Accepts a row name
            Put put = new Put(Bytes.toBytes("row1"));
            put.addColumn(Bytes.toBytes(CF_PERSONAL), Bytes.toBytes(COL_HERO), Bytes.toBytes("superman"));
            put.addColumn(Bytes.toBytes(CF_PERSONAL), Bytes.toBytes(COL_POWER), Bytes.toBytes("strength"));
            put.addColumn(Bytes.toBytes(CF_PROFESSIONAL), Bytes.toBytes(COL_NAME), Bytes.toBytes("clark"));
            put.addColumn(Bytes.toBytes(CF_PROFESSIONAL), Bytes.toBytes(COL_XP), Bytes.toBytes("100"));
            powers.put(put);

            put = new Put(Bytes.toBytes("row2"));
            put.addColumn(Bytes.toBytes(CF_PERSONAL), Bytes.toBytes(COL_HERO), Bytes.toBytes("batman"));
            put.addColumn(Bytes.toBytes(CF_PERSONAL), Bytes.toBytes(COL_POWER), Bytes.toBytes("money"));
            put.addColumn(Bytes.toBytes(CF_PROFESSIONAL), Bytes.toBytes(COL_NAME), Bytes.toBytes("bruce"));
            put.addColumn(Bytes.toBytes(CF_PROFESSIONAL), Bytes.toBytes(COL_XP), Bytes.toBytes("50"));
            powers.put(put);

            put = new Put(Bytes.toBytes("row3"));
            put.addColumn(Bytes.toBytes(CF_PERSONAL), Bytes.toBytes(COL_HERO), Bytes.toBytes("wolverine"));
            put.addColumn(Bytes.toBytes(CF_PERSONAL), Bytes.toBytes(COL_POWER), Bytes.toBytes("healing"));
            put.addColumn(Bytes.toBytes(CF_PROFESSIONAL), Bytes.toBytes(COL_NAME), Bytes.toBytes("logan"));
            put.addColumn(Bytes.toBytes(CF_PROFESSIONAL), Bytes.toBytes(COL_XP), Bytes.toBytes("75"));
            powers.put(put);

            // Close table
            powers.close();

            // Instantiate the Scan class
            Scan scan = new Scan();

            // Scan the required columns
            scan.addColumn(Bytes.toBytes(CF_PERSONAL), Bytes.toBytes(COL_HERO));

            /*
            scan.addColumn(Bytes.toBytes(CF_PERSONAL), Bytes.toBytes(COL_POWER));
            scan.addColumn(Bytes.toBytes(CF_PROFESSIONAL), Bytes.toBytes(COL_NAME));
            scan.addColumn(Bytes.toBytes(CF_PROFESSIONAL), Bytes.toBytes(COL_XP));
             */

            // Get the scan result
            ResultScanner scanner = powers.getScanner(scan);

            // Read values from scan result
            // Print scan result
            for (Result result = scanner.next(); result != null; result = scanner.next()) {
                System.out.println(result);
            }

            // Close the scanner
            scanner.close();

            // Htable closer
            powers.close();

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
}

