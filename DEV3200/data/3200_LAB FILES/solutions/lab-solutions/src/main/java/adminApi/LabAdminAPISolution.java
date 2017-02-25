/*
 * Copyright © 2013 MapR Technologies. All Rights Reserved.
 */
package adminApi;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class LabAdminAPISolution {

	// schema variables
	public static final String userdirectory = ".";
	public static final String tablePath = userdirectory + "/adminlabtrades";
	private static final byte[] adminTradesTableName = Bytes.toBytes(tablePath);

	// table 'adminlabtrades' with CFs: trades
	// Under CF 'trades' we have qualifiers 'price', 'vol' & 'time.
	private final static byte[] tradesCF = Bytes.toBytes("trades");
	private final static byte[] priceCol = Bytes.toBytes("price");
	private final static byte[] volumeCol = Bytes.toBytes("vol");

	private final static byte[] statsCF = Bytes.toBytes("stats");

	public static void main(String[] args) throws IOException {

		if (args.length > 0 && args[0].equalsIgnoreCase("setup")) {
			System.out.println("Setting up adminlabtrades Table ...");
			// TODO Step1: Compile and run this application and see what tables
			// are created
			// and what information is printed out
			setupTable(); // Creates the adminlabtrades table and sets some data
			// After you run the program, see how many trades data did we store
			// for CSCO and GOOG?
			// Do we see all of that data printed when we use scan and print?
			// How do we address this problem?
		} else if (args.length > 0
				&& args[0].equalsIgnoreCase("setupmaxversions")) {
			// TODO 2 Step2:
			// Complete the implementation in setupTablesMaxVersions to use
			// HTableDescriptor and change the number of versions stored to
			// Integer.MAX_VALUE
			setupTablesMaxVersions(); // Creates the adminlabtrades table and
										// sets max versions
		} else if (args.length > 0 && args[0].equalsIgnoreCase("presplit")) {
			// TODO 3 Step3 :
			// Complete the implementation in setupTablesPresplit to pre-split
			// the table
			setupTablesPresplit(); // Creates the adminlabtrades using presplit
		} else if (args.length > 0 && args[0].equalsIgnoreCase("listtables")) {
			// TODO 4 Step4:
			listTables();
		}

	}

	/**
	 * setupTable : In this method, we'll use the admin API to create tables
	 * using HTableDescriptor & HTable.
	 * 
	 * @throws IOException
	 * 
	 */
	public static void setupTable() throws IOException {

		Configuration conf = HBaseConfiguration.create();
		HBaseAdmin admin = new HBaseAdmin(conf);

		// TODO 1: Create table adminTradesTable to store the stock trades
		// with Column families trades & stats
		if (admin.tableExists(adminTradesTableName)) {
			System.out.println(" table already exists... so deleting it.");
			admin.disableTable(adminTradesTableName);
			admin.deleteTable(adminTradesTableName);
		}

		// TODO 1
		// Complete implementation here to create the tableDescriptor
		// with Column families trades & stats
		HTableDescriptor tableDescriptor = new HTableDescriptor(
				TableName.valueOf (adminTradesTableName));
		tableDescriptor.addFamily(new HColumnDescriptor(tradesCF)); // TODO
		tableDescriptor.addFamily(new HColumnDescriptor(statsCF)); // TODO
		admin.createTable(tableDescriptor);

		HTable adminTradesTable = new HTable(conf, adminTradesTableName);

		System.out
				.println("*****************************************************");
		System.out.println("1: Created table adminTradesTable... ");

		initTradesTable(adminTradesTable);
		printTradesTable(adminTradesTable);

		// remember to close the table to release all the resources
		adminTradesTable.close();
		admin.close();

	}

	public static void setupTablesMaxVersions() throws IOException {

		Configuration conf = HBaseConfiguration.create();
		HBaseAdmin admin = new HBaseAdmin(conf);

		// TODO 2: Create table adminTradesTable to store the stock trades
		// Here change column family property to store max versions for cells
		if (admin.tableExists(adminTradesTableName)) {
			System.out.println(" table already exists... so deleting it.");
			admin.disableTable(adminTradesTableName);
			admin.deleteTable(adminTradesTableName);
		}

		HTableDescriptor tableDescriptor = new HTableDescriptor(
				TableName.valueOf(adminTradesTableName));
		tableDescriptor.addFamily(new HColumnDescriptor(tradesCF));
		tableDescriptor.addFamily(new HColumnDescriptor(statsCF));

		HColumnDescriptor tradesColDesc[] = tableDescriptor.getColumnFamilies();
		for (HColumnDescriptor colDesc : tradesColDesc) {
			System.out.println(colDesc);
			System.out.println(" Max versions = " + colDesc.getMaxVersions());
			System.out.println(" Min versions = " + colDesc.getMinVersions());
			System.out.println(" Name = " + colDesc.getNameAsString());
			System.out.println(" TTL = " + colDesc.getTimeToLive());
			System.out.println(" BloomFilterType = "
					+ colDesc.getBloomFilterType());

			// TODO_2: Set Max versions property for HColumnDescriptor to
			// Integer.MAX_VALUE
			colDesc.setMaxVersions(Integer.MAX_VALUE);
			System.out.println(" Max versions = " + colDesc.getMaxVersions());

		}
		admin.createTable(tableDescriptor);

		// See docs at:
		// http://hbase.apache.org/0.94/apidocs/org/apache/hadoop/hbase/HColumnDescriptor.html
		HTable adminTradesTable = new HTable(conf, adminTradesTableName);

		HTableDescriptor tableDesc = adminTradesTable.getTableDescriptor();
		tradesColDesc = tableDesc.getColumnFamilies();
		for (HColumnDescriptor colDesc : tradesColDesc) {
			System.out.println(colDesc);
			System.out.println(" Max versions = " + colDesc.getMaxVersions());
			System.out.println(" Min versions = " + colDesc.getMinVersions());
			System.out.println(" Name = " + colDesc.getNameAsString());
			System.out.println(" TTL = " + colDesc.getTimeToLive());
			System.out.println(" BloomFilterType = "
					+ colDesc.getBloomFilterType());
			System.out.println(" Blocksize = " + colDesc.getBlocksize());
			System.out.println(" Compression Algorithm = "
					+ colDesc.getCompression().getName());
			System.out.println(" is InMemory = " + colDesc.isInMemory());
		}

		// TODO 2-a: In setupTable2, List compression options and change it
		System.out
				.println("*****************************************************");
		System.out
				.println("2: Created table adminTradesTable using MaxVersions ... ");

		initTradesTable(adminTradesTable);
		printTradesTable(adminTradesTable);

		// remember to close the table to release all the resources
		adminTradesTable.close();
		admin.close();

	}

	public static void setupTablesPresplit() throws IOException {

		Configuration conf = HBaseConfiguration.create();
		HBaseAdmin admin = new HBaseAdmin(conf);

		// TODO 3: Complete the implementation to create a pre-split the table
		// http://hbase.apache.org/0.94/apidocs/org/apache/hadoop/hbase/client/HBaseAdmin.html#createTable(org.apache.hadoop.hbase.HTableDescriptor,
		// byte[][])
		if (admin.tableExists(adminTradesTableName)) {
			System.out.println(" table already exists... so deleting it.");
			admin.disableTable(adminTradesTableName);
			admin.deleteTable(adminTradesTableName);
		}

		HTableDescriptor tableDescriptor = new HTableDescriptor(
				TableName.valueOf (	adminTradesTableName));
		tableDescriptor.addFamily(new HColumnDescriptor(tradesCF));
		tableDescriptor.addFamily(new HColumnDescriptor(statsCF));

		HColumnDescriptor tradesColDesc[] = tableDescriptor.getColumnFamilies();
		for (HColumnDescriptor colDesc : tradesColDesc) {
			System.out.println(colDesc);
			// System.out.println(" Max versions = " +
			// colDesc.getMaxVersions());
			colDesc.setMaxVersions(Integer.MAX_VALUE);
			// System.out.println(" Max versions = " +
			// colDesc.getMaxVersions());
		}
		// TODO 3:
		byte[][] splitKeys = new byte[][] { Bytes.toBytes("I"),
				Bytes.toBytes("P"), };

		admin.createTable(tableDescriptor, splitKeys);

		// See docs at:
		// http://hbase.apache.org/0.94/apidocs/org/apache/hadoop/hbase/HColumnDescriptor.html
		HTable adminTradesTable = new HTable(conf, adminTradesTableName);

		// HTableDescriptor tableDesc = adminTradesTable.getTableDescriptor();
		// tradesColDesc = tableDesc.getColumnFamilies();
		// for (HColumnDescriptor colDesc : tradesColDesc) {
		// System.out.println(colDesc);
		// System.out.println(" Max versions = " + colDesc.getMaxVersions());
		// System.out.println(" Min versions = " + colDesc.getMinVersions());
		// System.out.println(" Name = " + colDesc.getNameAsString());
		// System.out.println(" TTL = " + colDesc.getTimeToLive());
		// System.out.println(" BloomFilterType = " +
		// colDesc.getBloomFilterType());
		// }
		System.out
				.println("*****************************************************");
		System.out
				.println("3: Created table adminTradesTable using pre-split ... ");

		initTradesTable(adminTradesTable);
		printTradesTable(adminTradesTable);

		// remember to close the table to release all the resources
		adminTradesTable.close();
		admin.close();
	}

	public static void listTables() throws IOException {
		Configuration conf = HBaseConfiguration.create();
		HBaseAdmin admin = new HBaseAdmin(conf);

		// TODO_4: List & print table descriptors for the tables in your home
		// directory
		HTableDescriptor[] allTables = admin.listTables(userdirectory);
		System.out.println("Printing all tables...");
		for (HTableDescriptor tabledesc : allTables) {
			System.out.println(tabledesc.getNameAsString());
		}

		admin.close();
	}

	public static void printTradesTable(HTable adminTradesTable) {
		System.out.println("\nIn printTradesTable ...");

		try {
			// Display all the records for the table by doing a Scan
			Scan scan = new Scan();

			// TODO : Make sure we are getting all the versions of the data
			scan.setMaxVersions();

			ResultScanner scanner = adminTradesTable.getScanner(scan);
			System.out.println("************************************");
			System.out.println("Scan results for Table:");
			for (Result scanResult : scanner) {
				System.out.println(Tools.resultMapToString(scanResult));
			}
			// Remember to close the ResultScanner!
			scanner.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}



	public static void initTradesTable(HTable adminTradesTable)
			throws IOException {
		List<Trade> tradeList = generateDataSet();
		System.out
				.println("Initializing adminTradesTable in initTradesTable() ...");
		for (Trade trade : tradeList) {
			System.out.println("Putting trade: " + trade);
			String rowkey = trade.getSymbol();
			Put put = new Put(Bytes.toBytes(rowkey));
			put.add(tradesCF, priceCol, Bytes.toBytes(trade.getPrice()));
			put.add(tradesCF, volumeCol, Bytes.toBytes(trade.getVolume()));
			adminTradesTable.put(put);
		}
	}

	public static List<Trade> generateDataSet() {
		List<Trade> trades = new ArrayList<Trade>();
		// Params: String tradeSymbol, Float tradePrice, Long tradeVolume, Long
		// tradeTime
		trades.add(new Trade("AMZN", 304.66f, 1000l, 1381396363l * 1000));
		trades.add(new Trade("AMZN", 303.91f, 1600l, 1381397364l * 1000));
		trades.add(new Trade("AMZN", 304.82f, 1900l, 1381398365l * 1000));
		trades.add(new Trade("CSCO", 22.76f, 2300l, 1381399349l * 1000));
		trades.add(new Trade("CSCO", 22.78f, 250l, 1381399650l * 1000));
		trades.add(new Trade("CSCO", 22.80f, 1000l, 1381399951l * 1000));
		trades.add(new Trade("CSCO", 22.82f, 300l, 1381400252l * 1000));
		trades.add(new Trade("CSCO", 22.84f, 600l, 1381400553l * 1000));
		trades.add(new Trade("CSCO", 22.86f, 500l, 1381400854l * 1000));
		trades.add(new Trade("CSCO", 22.88f, 40l, 1381401155l * 1000));
		trades.add(new Trade("CSCO", 22.90f, 500000l, 1381401456l * 1000));
		trades.add(new Trade("CSCO", 22.92f, 100l, 1381401757l * 1000));
		trades.add(new Trade("CSCO", 22.94f, 5329l, 1381402058l));
		trades.add(new Trade("CSCO", 22.96f, 5662l, 1381402359l * 1000));
		trades.add(new Trade("CSCO", 22.98f, 5995l, 1381402660l * 1000));
		trades.add(new Trade("CSCO", 22.99f, 6328l, 1381402801l * 1000));
		trades.add(new Trade("GOOG", 867.24f, 7327l, 1381415776l * 1000));
		trades.add(new Trade("GOOG", 866.73f, 7660l, 1381416277l * 1000));
		trades.add(new Trade("GOOG", 866.22f, 7993l, 1381416778l * 1000));
		trades.add(new Trade("GOOG", 865.71f, 8326l, 1381417279l * 1000));
		trades.add(new Trade("GOOG", 865.20f, 8659l, 1381417780l * 1000));
		trades.add(new Trade("GOOG", 864.69f, 8992l, 1381418281l * 1000));
		trades.add(new Trade("GOOG", 864.18f, 9325l, 1381418782l * 1000));
		trades.add(new Trade("ORCL", 32.18f, 9325l, 1381418782l * 1000));
		trades.add(new Trade("ZNGA", 4.18f, 9000l, 1381418782l * 1000));
		return trades;
	}
}
