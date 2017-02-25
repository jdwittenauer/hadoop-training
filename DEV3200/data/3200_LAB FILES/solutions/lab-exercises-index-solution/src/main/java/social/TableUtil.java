package social;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class TableUtil {

	public static void deleteTable(Configuration conf, byte[] tableName)
			throws IOException {
		HBaseAdmin admin = new HBaseAdmin(conf);
		if (admin.tableExists(tableName)) {
			System.out.printf("Deleting %s\n", Bytes.toString(tableName));
			if (admin.isTableEnabled(tableName)) {
				admin.disableTable(tableName);
			}
			admin.deleteTable(tableName);
		}
		admin.close();
	}

	public static void createTable(Configuration conf, byte[] tableName,
			byte[] infoFam, int maxVersion) throws IOException {
		createTable(conf, tableName, infoFam, null, maxVersion);
	}

	public static void createTable(Configuration conf, byte[] tableName,
			byte[] infoFam, byte[] infoFam2, int maxVersion) throws IOException {
		HBaseAdmin admin = new HBaseAdmin(conf);
		if (admin.tableExists(tableName)) {
			System.out.println(" table already exists.");
		} else {
			System.out.println("Creating  table...");
			HTableDescriptor desc = new HTableDescriptor(
					TableName.valueOf(tableName));
			;
			HColumnDescriptor c = new HColumnDescriptor(infoFam);
			c.setMaxVersions(maxVersion);
			desc.addFamily(c);
			if (infoFam2 != null) {
				HColumnDescriptor c2 = new HColumnDescriptor(infoFam2);
				c2.setMaxVersions(maxVersion);
				desc.addFamily(c2);
			}
			admin.createTable(desc);
			System.out.println(" table created.");
		}
		admin.close();
	}

	/**
	 * 
	 * @param tableName
	 */
	public static void printTable(Configuration conf, byte[] tableName) {
		System.out.println("printTable");

		try {

			HTableInterface table = new HTable(conf, tableName);

			// Print all the records to the screen 
			Scan scan = new Scan();
			ResultScanner scanner = table.getScanner(scan);
			System.out
					.println("*****************************************************");
			System.out.println("Scan results for  Table: " + tableName);
			for (Result scanResult : scanner) {
				System.out.println(resultToString(scanResult));
			}
			// Remember to close the ResultScanner!
			scanner.close();
			table.close();
		} catch (IOException e) {
			System.out.println(e);
		}
	}

	public static String resultToString(Result result) {
		StringBuilder strBuilder = new StringBuilder();
		strBuilder.append("Result with rowKey "
				+ Bytes.toString(result.getRow()) + " : ");

		for (Cell kv : result.rawCells()) {
			strBuilder.append(" Family - "
					+ Bytes.toString(CellUtil.cloneFamily(kv)));
			strBuilder.append(" : Qualifier - "
					+ Bytes.toString(CellUtil.cloneQualifier(kv)));
			strBuilder.append(" : Value: "
					+ Bytes.toLong(CellUtil.cloneValue(kv)) + " ");
		}
		return strBuilder.toString();
	}
}
