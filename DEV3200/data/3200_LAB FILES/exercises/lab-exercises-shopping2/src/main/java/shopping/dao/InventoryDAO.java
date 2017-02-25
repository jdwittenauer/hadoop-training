package shopping.dao;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import shopping.model.Inventory;

public class InventoryDAO {

	// Schema variables for Inventory Table
	// public static final String userdirectory
	// =System.getProperty("user.home");
	public static final String userdirectory = ".";
	public static final String tableName = userdirectory + "/Inventory";
	public static final byte[] TABLE_NAME = Bytes.toBytes(tableName);
	// Rowkey for inventoryTable is productName: pen, notepad, eraser ...
	// Column Family is 'stock'
	public static final byte[] STOCK_CF = Bytes.toBytes("stock");
	// column 'quantity'
	public static final byte[] QUANTITY_COL = Bytes.toBytes("quantity");
	Configuration conf = null;
	private HTableInterface table = null;
	private static final Logger log = Logger.getLogger(InventoryDAO.class);

	public InventoryDAO(HTableInterface tableInterface) {
		this.table = tableInterface;
	}

	public InventoryDAO(Configuration conf) throws IOException {
		this.conf = conf;
		this.table = new HTable(conf, TABLE_NAME);
	}

	public Get mkGet(String stock) throws IOException {
		log.debug(String.format("Creating Get for %s", stock));
		Get g = new Get(Bytes.toBytes(stock));
		g.addFamily(STOCK_CF);
		System.out.println("mkGet   [rowkey= " + stock + "]");
		return g;
	}

	public Put mkPut(Inventory inv) {
		log.debug(String.format("Creating Put for %s", inv));
		Put p = new Put(Bytes.toBytes(inv.stock)); //
		p.add(STOCK_CF, QUANTITY_COL, Bytes.toBytes(inv.quantity));
		System.out.println("mkPut   [" + inv + "]");
		return p;
	}

	public boolean checkout(String stockId, String cartId, long amt)
			throws Exception {
		// TODO 2a finish checkout code
		boolean ok = false;
		System.out.println("--checkout Inventory stockId " + stockId
				+ " cartId " + cartId + " quantity " + amt);
		Inventory inventory = getInventory(stockId);
		long currentQuantity = inventory.quantity;
		long newQuantity = currentQuantity - amt;
		if (newQuantity < 0)
			System.out.println("Not enougn inventory for : " + stockId);
		else {
		
			byte[] userCol = Bytes.toBytes(cartId);
			Put p = new Put(Bytes.toBytes(stockId)); //
			p.add(STOCK_CF, QUANTITY_COL, Bytes.toBytes(newQuantity));
			// add column for user cartId and amt :
			// TODO 2a call put.add with STOCK_CF, userCol, Bytes.toBytes(amt)
			// TODO 2a call table.checkAndPut with
			// (Bytes.toBytes(stockId), STOCK_CF, QUANTITY_COL,
			// Bytes.toBytes(currentQuantity), put);

			System.out.println(" result  " + ok);
			if (ok) {
				System.out
						.println("--checkout success: Changed " + stockId
								+ " quantity " + currentQuantity + " to "
								+ newQuantity);
				System.out.println("--added column for cartId " + cartId
						+ " quantiy " + amt);
			}
		
		}
		return ok;
	}

	public void putInventoryList(List<Put> puts) throws IOException {
		// TODO 1a finish code

		// TODO call table.put with puts list

	}

	public void putInventoryBatch(List<Put> puts) throws Exception {
		// TODO 1b finish code

		Object[] results = new Object[puts.size()];
		// TODO 1b call table.batch with puts results
		for (Object result : results) {
			System.out.println("batch put result: " + result);
		}

	}

	public Put mkPutVersion(String stock, long version, long quantity) {
		log.debug(String.format("Creating Put for %s", stock));
		Put p = new Put(Bytes.toBytes(stock), version); //
		p.add(STOCK_CF, QUANTITY_COL, Bytes.toBytes(quantity));
		System.out.println("mkPut   [rowkey=" + stock + ", quantity= "
				+ quantity + "]");
		return p;
	}

	public Put mkPut(String stock, long quantity) {
		log.debug(String.format("Creating Put for %s", stock));
		Put p = new Put(Bytes.toBytes(stock)); //
		p.add(STOCK_CF, QUANTITY_COL, Bytes.toBytes(quantity));
		System.out.println("mkPut   [rowkey=" + stock + ", quantity= "
				+ quantity + "]");
		return p;
	}

	public Put mkPut(String stock, byte[] fam, byte[] qual, byte[] val) {
		Put p = new Put(Bytes.toBytes(stock));
		p.add(fam, qual, val);
		return p;
	}

	public Delete mkDel(String stock) {
		log.debug(String.format("Creating Delete for %s", stock));
		Delete d = new Delete(Bytes.toBytes(stock));
		System.out.println("mkDel  [rowkey= " + stock + "]");
		return d;
	}

	private Scan mkScan() {
		Scan s = new Scan();
		s.addFamily(STOCK_CF);
		System.out.println("mkScan for Inventory table ");
		return s;
	}

	public void checkInventoryQuantityAndPut(String stock, long quantity,
			Put put) throws IOException {

		table.checkAndPut(Bytes.toBytes(stock), STOCK_CF, QUANTITY_COL,
				Bytes.toBytes(quantity), put);

	}

	public Result increment(Increment inc) throws IOException {

		Result result = table.increment(inc);

		return result;
	}

	public void addInventory(String stock, long quantity) throws IOException {

		Put p = mkPut(new Inventory(stock, quantity));
		table.put(p);

	}



	public Inventory getInventory(String stockId) throws IOException {

		Result result = getInventoryRow(stockId);
		// create Inventory Model Object from Get Result
		Inventory inv = createInventory(result);
		// close Htable interface
		return inv;
	}

	public Result getInventoryRow(String stockId) throws IOException {

		// make a get object
		Get g = mkGet(stockId);
		// call htable.get with get object
		Result result = table.get(g);
		if (result.isEmpty()) {
			log.info(String.format("Inventory %s not found.", stockId));
			return null;
		}
		// System.out.println("Get Inventory Result :");
		// System.out.println(resultToString(result));

		return result;
	}

	public void deleteInventory(String id) throws IOException {

		// make a delete object
		Delete d = mkDel(id);
		// call htable.delete with delete object
		table.delete(d);
		// close Htable interface

	}

	public List<Inventory> getInventorys() throws IOException {

		// make a scan object
		Scan scan = mkScan();
		// call htable.getScanner with scan object
		ResultScanner results = table.getScanner(scan);
		// create a list of Inventory objects to return
		ArrayList<Inventory> inventoryList = new ArrayList<Inventory>();
		System.out.println("Scan Inventory Results :");
		for (Result result : results) {
			System.out.println(Tools.resultMapToString(result));
			// create Inventory Model Object from Result
			Inventory inv = createInventory(result);
			// add inventory object to list
			inventoryList.add(inv);
		}

		// return list of inventory objects
		return inventoryList;
	}

	/*
	 * create Inventory Model Object from Get or Scan Result
	 */
	public Inventory createInventory(Result r) {
		// call Inventory constructor with row key and quantity
		// Inventory(byte[] stock, byte[] quantity)
		Inventory inv = new Inventory(r.getRow(), r.getValue(STOCK_CF,
				QUANTITY_COL));
		return inv;
	}

	public Inventory createInventory(Cell kv) {
		// call Inventory constructor with row key and quantity
		// Inventory(byte[] stock, byte[] quantity)
		Inventory inv = new Inventory(kv.getRowArray(), kv.getValueArray());
		return inv;
	}

	public static String resultToString(byte[] row, byte[] family,
			byte[] qualifier, byte[] value) {
		StringBuilder strBuilder = new StringBuilder();
		strBuilder.append("Result with rowKey " + Bytes.toString(row) + " : ");
		strBuilder.append(" Family - " + Bytes.toString(family));
		strBuilder.append(" : Qualifier - " + Bytes.toString(qualifier));
		strBuilder.append(" : Value: " + Bytes.toLong(value));
		return strBuilder.toString();
	}

	public void close() throws IOException {
		table.close();
	}
}
