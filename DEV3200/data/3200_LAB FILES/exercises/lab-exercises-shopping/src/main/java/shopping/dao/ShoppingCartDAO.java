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

    private static Get mkGet(String cartId) throws IOException {
        log.debug(String.format("Creating Get for %s", cartId));
	// TODO 2a complete the get operation to save data
        // create Get Object passing rowkey
        // use Bytes.toBytes on rowkey cartId
        Get get = null; // TODO 2a
        // TODO 2a call get addFamily passing column family ITEMS_CF
        System.out.println("mkGet Shoppingcart key  [" + cartId + "]");
        return get;
    }

    private static Put mkPut(ShoppingCart cart) {
        log.debug(String.format("Creating Put for %s", cart));
	// TODO 2a Complete the put operation to save data
        // create Put Object passing rowkey
        // use Bytes.toBytes on rowkey cart.cartId
        Put put = null; // TODO 2a create Put Object with cart.cartId in Bytes
        put.add(ITEMS_CF, PEN_COL, Bytes.toBytes(cart.pens));
	// TODO 2a call put.add for notepad column: NOTEPAD_COL cart.notepads
        // TODO 2a call put.add for eraser column: ERASER_COL cart.erasers
        System.out.println("mkPut Shoppingcart  [" + cart + "]");
        return put;
    }

    private static Delete mkDel(String cartId) {
        log.debug(String.format("Creating Delete for %s", cartId));
	// TODO 3a finish code
        // TODO 3a create Delete object passing cartId in Bytes
        Delete d = null; // TODO 3a
        return d;
    }

    private static Scan mkScan() {
        System.out.println("mkScan Shoppingcart ");
        // TODO 2b Complete the code below
        Scan scan = null; // TODO 2b create Scan object
        // TODO 2b call scan addFamily passing column family ITEMS_CF
        return scan;
    }

    public void addShoppingCart(String cartId, long pens, long notepads, long erasers)
            throws IOException {
        // TODO 2a Complete the mkPut
        Put p = mkPut(new ShoppingCart(cartId, pens, notepads, erasers));
        // TODO 2a call table.put (passing put object)
        table.close();
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
        table.close();
        return s;
    }



    public void deleteShoppingCart(String cart) throws IOException {
        // TODO 3a finish code in mkDel
        Delete delete = mkDel(cart);
        // TODO 3a call table delete() with delete object
        table.close();
    }

    public List<ShoppingCart> getShoppingCarts() throws IOException {
        // TODO 2b Complete the code in mkScan
        Scan scan = mkScan();
        // TODO 2b Call table getScanner with scan object
        ResultScanner results = null; // TODO 2b call table.getScanner(scan)
        ArrayList<ShoppingCart> listOfCarts = new ArrayList<ShoppingCart>();
        System.out.println("Scan ShoppingCart Results:");
	// TODO 2b Complete the code below
        // TODO 2b iterate through the results
        // TODO 2b HINT for (Result result : results) {
        // System.out.println(resultToString(result));
        // call createShoppingCart with a single result
        ShoppingCart cart = null; // TODO 2b createShoppingCart(result)
        // add cart to list to return
        listOfCarts.add(cart);
        // }
        table.close();
        return listOfCarts;
    }

    private ShoppingCart createShoppingCart(Result result) {
	// TODO 2a Complete
        // Instantiate a ShoppingCart object using this constructor:
        // ShoppingCart(byte[] cartId, byte[] pens, byte[] notepads, byte[]
        // erasers)
        // get the cartId from the result with result.getRow()
        // get the pens value using result.getValue(ITEMS_CF,PEN_COL)
        // get the notepads value using result.getValue(ITEMS_CF, NOTEPAD_COL)
        // get erasers using result.getValue(ITEMS_CF, ERASER_COL)
        ShoppingCart shoppingCart = null; // TODO 2a
        return shoppingCart;
    }

	public void close() throws IOException {
		table.close();
	}

}
