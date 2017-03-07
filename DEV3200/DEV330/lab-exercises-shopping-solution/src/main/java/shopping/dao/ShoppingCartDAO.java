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
	public static final String userdirectory = ".";
	public static final String tableName = userdirectory + "/Shoppingcart";
	public static final byte[] TABLE_NAME = Bytes.toBytes(tableName);
	public static final byte[] ITEMS_CF = Bytes.toBytes("items");
	public static byte[] PEN_COL = Bytes.toBytes("pens");
	public static byte[] NOTEPAD_COL = Bytes.toBytes("notepads");
	public static byte[] ERASER_COL = Bytes.toBytes("erasers");
	private static final Logger log = Logger.getLogger(ShoppingCartDAO.class);
	private HTableInterface table = null;

	public ShoppingCartDAO(HTableInterface tableInterface) {
		this.table = tableInterface;
	}

	public ShoppingCartDAO(Configuration conf) throws IOException {
		this.table = new HTable(conf, TABLE_NAME);
	}

	private static Get mkGet(String cart) throws IOException {
		log.debug(String.format("Creating Get for %s", cart));
		Get g = new Get(Bytes.toBytes(cart));
		g.addFamily(ITEMS_CF);
		System.out.println("mkGet Shoppingcart key  [" + cart + "]");
		return g;
	}

	private static Put mkPut(ShoppingCart u) {
		log.debug(String.format("Creating Put for %s", u));
		Put p = new Put(Bytes.toBytes(u.cartId)); //
		p.add(ITEMS_CF, PEN_COL, Bytes.toBytes(u.pens));
		p.add(ITEMS_CF, NOTEPAD_COL, Bytes.toBytes(u.notepads));
		p.add(ITEMS_CF, ERASER_COL, Bytes.toBytes(u.erasers));
		System.out.println("mkPut Shoppingcart  [" + u + "]");
		return p;
	}

	private static Delete mkDel(String cartId) {
		log.debug(String.format("Creating Delete for %s", cartId));
		Delete d = new Delete(Bytes.toBytes(cartId));
		return d;
	}

	private static Scan mkScan() {
		System.out.println("mkScan Shoppingcart ");
		Scan s = new Scan();
		s.addFamily(ITEMS_CF);
		return s;
	}

	public void addShoppingCart(String cartId, long pens, long notepads, long erasers) throws IOException {
		Put p = mkPut(new ShoppingCart(cartId, pens, notepads, erasers));
		table.put(p);
	}

	public ShoppingCart getShoppingCart(String cartId) throws IOException {
		Get g = mkGet(cartId);
		Result result = table.get(g);

		if (result.isEmpty()) {
			log.info(String.format("cart %s not found.", cartId));
			return null;
		}

		System.out.println("Get ShoppingCart Result :");
		System.out.println(Tools.resultMapToString(result));
		ShoppingCart s = createShoppingCart(result);
		return s;
	}

	public void deleteShoppingCart(String cart) throws IOException {
		Delete d = mkDel(cart);
		table.delete(d);
	}

	public List<ShoppingCart> getShoppingCarts() throws IOException {
		ResultScanner results = table.getScanner(mkScan());
		ArrayList<ShoppingCart> ret = new ArrayList<ShoppingCart>();
		System.out.println("Scan ShoppingCart Results:");

		for (Result r : results) {
			System.out.println(Tools.resultToString(r));
			ShoppingCart cart = createShoppingCart(r);
			ret.add(cart);
		}

		return ret;
	}

	private ShoppingCart createShoppingCart(Result r) {
		ShoppingCart shoppingCart = new ShoppingCart(r.getRow(), r.getValue(
				ITEMS_CF, PEN_COL), r.getValue(ITEMS_CF, NOTEPAD_COL),
				r.getValue(ITEMS_CF, ERASER_COL));
		return shoppingCart;
	}

	public void close() throws IOException {
		table.close();
	}
}
