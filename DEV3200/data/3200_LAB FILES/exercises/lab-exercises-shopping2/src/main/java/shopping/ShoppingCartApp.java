package shopping;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

import shopping.dao.InventoryDAO;
import shopping.dao.ShoppingCartDAO;
import shopping.dao.Tools;
import shopping.model.Inventory;
import shopping.model.ShoppingCart;

public class ShoppingCartApp {

	/**
	 * This objective of this lab exercise is to: 1) Save data to the HBase
	 * tables using put operation 2) Retrieve and print the data from the HBase
	 * tables using get & scan operations and Result object. 3) Use Put List to
	 * batch them and also use write buffer for single puts. 4) remove rows and
	 * columns from a Table.
	 * 
	 * @author Sridhar Reddy
	 */
	public static void main(String[] args) throws Exception {

		Configuration conf = HBaseConfiguration.create();
		// Creates the tables
		createTables(conf);
		InventoryDAO inventoryDAO = new InventoryDAO(conf);
		ShoppingCartDAO shoppingCartDAO = new ShoppingCartDAO(conf);

		if (args.length > 0 && args[0].equalsIgnoreCase("setuplist")) {
			System.out.println("setuparraylist...");
			initInventoryTableList(inventoryDAO);
			
		} else if (args.length > 0 && (args[0].contains("init"))
				|| (args[0].contains("setup"))) {
			System.out.println("in init ...");
			// put data in Inventory table
			saveInventoryTableData(inventoryDAO);
			// print data in Inventory table
			printInventoryTable(inventoryDAO);
			// put data in Shopping table
			saveShoppingCartData(shoppingCartDAO);
			// print data in Shopping table
			printShoppingcartTable(shoppingCartDAO);

		} else if (args.length > 1 && args[0].equalsIgnoreCase("checkout")) {
			// TODO 2b run checkout
			checkout(inventoryDAO, shoppingCartDAO, args[1]);
		} else if (args.length > 1 && args[0].equalsIgnoreCase("delete")) {
			System.out.println("Deleting user from shoppingcart Table ..."
					+ args[1]);
			deleteUserCart(shoppingCartDAO, args[1]);

		} else if (args.length == 1 && args[0].equalsIgnoreCase("help")) {
			System.out
					.println("Usage for setup: shopping.ShoppingCartApp setup ");
		}
	}

	public static void createTables(Configuration conf) throws IOException {

		HBaseAdmin admin = new HBaseAdmin(conf);
		// 1. Create the two tables
		// Table 'Inventory'
		// Table 'Shoppingcart'
		createTables(admin);
		admin.close();
	}

	/*
	 * Method to save data to Inventory Table
	 */
	public static void saveInventoryTableData(InventoryDAO dao)
			throws IOException {
		// Add data in Inventory table
		System.out.println("------------------------------");
		System.out.println(" Inserting rows in Inventory Table: ");
		dao.addInventory("pens", 9);
		dao.addInventory("notepads", 21);
		dao.addInventory("erasers", 10);
	}

	/**
	 * Deletes the Inventory and Shoppingcart Tables
	 */
	private static void deleteTables(HBaseAdmin admin) throws IOException {
		InitTables.deleteTable(admin, ShoppingCartDAO.TABLE_NAME);
		InitTables.deleteTable(admin, InventoryDAO.TABLE_NAME);
	}

	/**
	 * Creates the Inventory and Shoppingcart Tables
	 */
	private static void createTables(HBaseAdmin admin) throws IOException {
		// Table 'Inventory' with family 'stock'
		InitTables.createTable(admin, InventoryDAO.TABLE_NAME,
				InventoryDAO.STOCK_CF, 3);
		// Table 'Shoppingcart' with family 'items'
		InitTables.createTable(admin, ShoppingCartDAO.TABLE_NAME,
				ShoppingCartDAO.ITEMS_CF, 3);
	}

	public static void initInventoryTableList(InventoryDAO dao)
			throws IOException {

		// TODO 3: Use list to put and get the data
		// Using ArrayList of Puts
		List<Put> puts = new ArrayList<Put>();

		System.out.println("------------------------------");
		System.out.println(" Inserting rows in Inventory Table: ");
		puts.add(dao.mkPut("pens", 9));
		puts.add(dao.mkPut("notepads", 21));
		puts.add(dao.mkPut("erasers", 10));
		System.out.println("Using ArrayList for puts ...");
		dao.putInventoryList(puts);
		System.out.println("Print");
		printInventoryTable(dao);

		// TODO 4: Use batch to put the data instead of list.
		// Note that List still uses round trips per each put.
		// Use batch instead to optimize.
		// Complete implementation here ...
		try {
			// TODO 4 Using batch for puts
			System.out.println("Using batch puts ...");
			puts = new ArrayList<Put>();
			puts.add(dao.mkPut("pens", 10));
			puts.add(dao.mkPut("notepads", 22));
			puts.add(dao.mkPut("erasers", 11));
			// Complete implementation in Inventory DAO
			System.out.println("Put Batch ...");
			dao.putInventoryBatch(puts);
			System.out.println("Print");
			printInventoryTable(dao);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/**
     * 
     */
	public static void addInventory(InventoryDAO dao, String items,
			String quantity) throws IOException {
		dao.addInventory(items, Long.parseLong(quantity));
	}

	public static void deleteUserCart(ShoppingCartDAO dao, String user)
			throws IOException {
		printShoppingcartTable(dao);
		System.out.println("Delete Cart for " + user);
		dao.deleteShoppingCart(user);
		printShoppingcartTable(dao);

	}

	/**
	 * Method to initialize Shoppingcart data
	 */
	public static void saveShoppingCartData(ShoppingCartDAO dao)
			throws IOException {
		dao.addShoppingCart("Mike", 1, 2, 3);
		dao.addShoppingCart("Mary", 1, 2, 5);
		dao.addShoppingCart("Adam", 5, 4, 2);
	}

	public static void printInventoryTable(InventoryDAO dao) throws IOException {
		System.out.println("Get Inventory From Inventory Table");
		List<Inventory> list = dao.getInventorys();
		System.out
				.println("*****************************************************");
		System.out.println("print Inventorys from Table ...");
		for (Inventory inventory : list) {
			System.out.println(inventory);
		}
	}

	public static void printShoppingcartTable(ShoppingCartDAO dao)
			throws IOException {
		System.out.println("Scan Shopping Cart Table");
		List<ShoppingCart> list = dao.getShoppingCarts();
		System.out
				.println("*****************************************************");
		if (list.isEmpty()) {
			System.out.println("Table is empty");
		} else {
			System.out.println("print Shoppingcart Table");
			for (ShoppingCart cart : list) {
				System.out.println(cart);
			}
		}
	}

	public static void checkout(InventoryDAO inventoryDAO,
			ShoppingCartDAO shoppingCartDAO, String cartId) throws Exception {
		// get cart from Shopping Cart Table for cartId
		System.out.println("Checkout for CartId :  " + cartId);
		ShoppingCart cart = shoppingCartDAO.getShoppingCart(cartId);
		// TODO 2a finish checkout code
		ShoppingCartApp.checkout(inventoryDAO, cart);
	}

	public static void checkout(InventoryDAO inventoryDAO, ShoppingCart cart)
			throws Exception {
		// get cart from Shopping Cart Table for cartId
		System.out.println("Checkout for Cart:  ");
		System.out.println(cart);
		// Checkout for pens
		// TODO 2a finish checkout code in Inventory DAO
		inventoryDAO.checkout("pens", cart.cartId, cart.pens);
		Result result = inventoryDAO.getInventoryRow("pens");
		System.out.println("Pens Inventory row "
				+ Tools.resultMapToString(result));
		// Process Notepads
		// TODO 2a call inventoryDAO.checkout for notepads

		result = inventoryDAO.getInventoryRow("notepads");
		System.out.println("Notepads Inventory row "
				+ Tools.resultMapToString(result));
		// Process Erasers
		// TODO 2a call inventoryDAO.checkout for erasers

		result = inventoryDAO.getInventoryRow("erasers");
		System.out.println("Notepads Inventory row "
				+ Tools.resultMapToString(result));

	}
}
