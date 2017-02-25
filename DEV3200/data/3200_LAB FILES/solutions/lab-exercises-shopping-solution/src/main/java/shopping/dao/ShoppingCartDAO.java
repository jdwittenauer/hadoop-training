package shopping.dao;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import shopping.model.ShoppingCart;



public class ShoppingCartDAO {

	//public static final String userdirectory =System.getProperty("user.home");
	public static final String userdirectory = ".";
	public static final String tableName = userdirectory + "/Shoppingcart";
	// Schema variables for ShoppingCart Table
	public static final byte[] TABLE_NAME = Bytes.toBytes(tableName);
	// column family: items
	public static final byte[] ITEMS_CF = Bytes.toBytes("items");
	// Rowkey is user: mike...
	// columns: pens, notepads, erasers
	public static byte[] PEN_COL = Bytes.toBytes("pens");
	public static byte[] NOTEPAD_COL = Bytes.toBytes("notepads");
	public static byte[] ERASER_COL = Bytes.toBytes("erasers");
	private static final Logger log = Logger.getLogger(ShoppingCartDAO.class);
	private HTableInterface table = null;

	public ShoppingCartDAO(HTableInterface tableInterface) {
		this.table = tableInterface;
	}

	public ShoppingCartDAO(Configuration conf) throws IOException{
		this.table = new HTable(conf, TABLE_NAME);
	}

	private static Get mkGet(String cart) throws IOException {
		log.debug(String.format("Creating Get for %s", cart));
		// TODO 2a complete the put operation to save data
		Get g = new Get(Bytes.toBytes(cart));
		g.addFamily(ITEMS_CF);
		System.out.println("mkGet Shoppingcart key  [" + cart + "]");
		return g;
	}

	private static Put mkPut(ShoppingCart u) {
		log.debug(String.format("Creating Put for %s", u));
		// TODO 2a Complete the put operation to save data
		Put p = new Put(Bytes.toBytes(u.cartId)); //
		p.add(ITEMS_CF, PEN_COL, Bytes.toBytes(u.pens));
		p.add(ITEMS_CF, NOTEPAD_COL, Bytes.toBytes(u.notepads));
		p.add(ITEMS_CF, ERASER_COL, Bytes.toBytes(u.erasers));
		System.out.println("mkPut Shoppingcart  [" + u + "]");
		return p;
	}

	private static Delete mkDel(String cartId) {
		log.debug(String.format("Creating Delete for %s", cartId));
		// TODO 3a finish code
		Delete d = new Delete(Bytes.toBytes(cartId));
		return d;
	}

	private static Scan mkScan() {
		System.out.println("mkScan Shoppingcart ");
		// TODO 2b Complete the code below
		Scan s = new Scan();
		s.addFamily(ITEMS_CF);
		return s;
	}

	public void addShoppingCart(String cartId, long pens, long notepads,
			long erasers) throws IOException {
		// TODO 2a Complete the mkPut
		Put p = mkPut(new ShoppingCart(cartId, pens, notepads, erasers));
		table.put(p);
	}

	public ShoppingCart getShoppingCart(String cartId) throws IOException {
		// TODO 2a Complete the mkGet
		Get g = mkGet(cartId);
		Result result = table.get(g);
		if (result.isEmpty()) {
			log.info(String.format("cart %s not found.", cartId));
			return null;
		}
		System.out.println("Get ShoppingCart Result :");
		System.out.println(Tools.resultMapToString(result));
		// TODO 2a Complete the createShoppingCart method
		ShoppingCart s = createShoppingCart(result);
		return s;
	}


	public void deleteShoppingCart(String cart) throws IOException {
		// TODO 3a finish code in mkDel
		Delete d = mkDel(cart);
		table.delete(d);
	}

	public List<ShoppingCart> getShoppingCarts() throws IOException {
		// TODO 2b Complete the code in mkScan
		ResultScanner results = table.getScanner(mkScan());
		ArrayList<ShoppingCart> ret = new ArrayList<ShoppingCart>();
		// TODO 2b Complete the code below
		System.out.println("Scan ShoppingCart Results:");
		for (Result r : results) {
			System.out.println(Tools.resultToString(r));
			ShoppingCart cart = createShoppingCart(r);
			ret.add(cart);
		}
		return ret;
	}

	private ShoppingCart createShoppingCart(Result r) {
		// TODO 2a Complete
		ShoppingCart shoppingCart = new ShoppingCart(r.getRow(), r.getValue(
				ITEMS_CF, PEN_COL), r.getValue(ITEMS_CF, NOTEPAD_COL),
				r.getValue(ITEMS_CF, ERASER_COL));
		return shoppingCart;
	}
	public void close() throws IOException {
		table.close();
	}


}
