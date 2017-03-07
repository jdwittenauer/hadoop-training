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
	public static final String userdirectory = ".";
	public static final String tableName = userdirectory + "/Inventory";
	public static final byte[] TABLE_NAME = Bytes.toBytes(tableName);
	public static final byte[] STOCK_CF = Bytes.toBytes("stock");
	public static final byte[] QUANTITY_COL = Bytes.toBytes("quantity");
	private HTableInterface table = null;
	private static final Logger log = Logger.getLogger(InventoryDAO.class);

	public InventoryDAO(HTableInterface tableInterface) {
		this.table = tableInterface;
	}

	public InventoryDAO(Configuration conf) throws IOException {
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
		Put p = new Put(Bytes.toBytes(inv.stock));
		p.add(STOCK_CF, QUANTITY_COL, Bytes.toBytes(inv.quantity));
		System.out.println("mkPut   [" + inv + "]");
		return p;
	}

	public Put mkPutVersion(String stock, long version, long quantity) {
		log.debug(String.format("Creating Put for %s", stock));
		Put p = new Put(Bytes.toBytes(stock), version);
		p.add(STOCK_CF, QUANTITY_COL, Bytes.toBytes(quantity));
		System.out.println("mkPut   [rowkey=" + stock + ", quantity= " + quantity + "]");
		return p;
	}

	public Put mkPut(String stock, long quantity) {
		log.debug(String.format("Creating Put for %s", stock));
		Put p = new Put(Bytes.toBytes(stock));
		p.add(STOCK_CF, QUANTITY_COL, Bytes.toBytes(quantity));
		System.out.println("mkPut   [rowkey=" + stock + ", quantity= " + quantity + "]");
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

	public void checkInventoryQuantityAndPut(String stock, long quantity, Put put) throws IOException {
		table.checkAndPut(Bytes.toBytes(stock), STOCK_CF, QUANTITY_COL, Bytes.toBytes(quantity), put);
	}

	public Result increment(Increment inc) throws IOException {
		Result result = table.increment(inc);
		return result;
	}

	public void addInventory(String stock, long quantity) throws IOException {
		Put p = mkPut(new Inventory(stock, quantity));
		table.put(p);
	}

	public Inventory getInventory(String stock) throws IOException {	
		// make a get object
		Get g = mkGet(stock);

		// call htable.get with get object
		Result result = table.get(g);

		if (result.isEmpty()) {
			log.info(String.format("Inventory %s not found.", stock));
			return null;
		}

		System.out.println("Get Inventory Result :");
		System.out.println(Tools.resultMapToString(result));

		// create Inventory Model Object from Get Result
		Inventory inv = createInventory(result);
		return inv;
	}

	public void deleteInventory(String cart) throws IOException {
		// make a delete object
		Delete d = mkDel(cart);

		// call htable.delete with delete object
		table.delete(d);
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
			System.out.println(Tools.resultToString(result));

			// create Inventory Model Object from Result
			Inventory inv = createInventory(result);

			// add inventory object to list
			inventoryList.add(inv);
		}

		table.close();

		// return list of inventory objects
		return inventoryList;
	}

	public List<Inventory> getInventorysMaxVersions() throws IOException {
		// make a scan object
		Scan scan = mkScan();

		// set Max Versions to return 3 versions
		scan.setMaxVersions(3);

		// call htable.getScanner with scan object
		ResultScanner results = table.getScanner(scan);

		// create a list of Inventory objects to return
		ArrayList<Inventory> inventoryList = new ArrayList<Inventory>();
		System.out.println("Scan Inventory Results :");

		for (Result result : results) {
			// get all KeyValues from Result (3 versions)
			for (Cell kv : result.rawCells()) {
				System.out.println("KeyValue :");
				System.out.println(kv);

				// create Inventory Model Objects for each KeyValue
				Inventory inv = createInventory(kv);

				// add inventory object to list
				inventoryList.add(inv);
			}
		}

		// return list of inventory objects
		return inventoryList;
	}

	public Inventory createInventory(Result r) {
		// call Inventory constructor with row key and quantity
		Inventory inv = new Inventory(r.getRow(), r.getValue(STOCK_CF, QUANTITY_COL));
		return inv;
	}

	public Inventory createInventory(Cell kv) {
		// call Inventory constructor with row key and quantity
		Inventory inv = new Inventory(kv.getRowArray(), kv.getValueArray());
		return inv;
	}

	public void close() throws IOException {
		table.close();
	}
}
