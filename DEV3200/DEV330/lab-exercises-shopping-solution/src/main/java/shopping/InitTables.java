package shopping;

import java.io.IOException;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;


public class InitTables {
    public static void deleteTable(HBaseAdmin admin, byte[] tableName) throws IOException {
        if (admin.tableExists(tableName)) {
            System.out.printf("Deleting %s\n", Bytes.toString(tableName));

            if (admin.isTableEnabled(tableName)) {
                admin.disableTable(tableName);
            }

            admin.deleteTable(tableName);
        }
    }

    public static void createTable(HBaseAdmin admin, byte[] tableName, byte[] infoFam, int maxVersion) throws IOException {
        if (admin.tableExists(tableName)) {
            System.out.println(" table already exists.");
        } 
        else {
            System.out.println("Creating  table...");
            HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
            HColumnDescriptor c = new HColumnDescriptor(infoFam);
            c.setMaxVersions(maxVersion);
            desc.addFamily(c);
            admin.createTable(desc);
            System.out.println(" table created.");
        }
    }
}
