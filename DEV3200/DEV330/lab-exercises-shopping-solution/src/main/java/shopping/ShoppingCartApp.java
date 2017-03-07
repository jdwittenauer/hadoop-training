package shopping;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import shopping.dao.InventoryDAO;
import shopping.dao.ShoppingCartDAO;
import shopping.model.Inventory;
import shopping.model.ShoppingCart;


public class ShoppingCartApp {
	public static void main(String[] args) throws IOException {
		Configuration conf = HBaseConfiguration.create();
		createTables(conf);
		InventoryDAO inventoryDAO = new InventoryDAO(conf);
		ShoppingCartDAO shoppingCartDAO = new ShoppingCartDAO(conf);

		if (args.length > 0 && (args[0].contains("init"))||(args[0].equalsIgnoreCase("setup"))) {
			System.out.println("in init ...");
			saveInventoryTableData(inventoryDAO);
			printInventoryTable(inventoryDAO);
			saveShoppingCartData(shoppingCartDAO);
			printShoppingcartTable(shoppingCartDAO);
		}

		if (args.length > 1 && args[0].equalsIgnoreCase("delete")) {
			System.out.println("Deleting user from shoppingcart Table ..." + args[1]);
			deleteUserCart(shoppingCartDAO, args[1]);
		}

		if (args.length == 1 && args[0].equalsIgnoreCase("help")) {
			System.out.println("Usage for setup: shopping.ShoppingCartApp setup");
		}

		inventoryDAO.close();
		shoppingCartDAO.close();
	}

	public static void createTables(Configuration conf) throws IOException {
		HBaseAdmin admin = new HBaseAdmin(conf);
		createTables(admin);
		admin.close();
	}

	public static void resetTables(Configuration conf) throws IOException {
		HBaseAdmin admin = new HBaseAdmin(conf);
		deleteTables(admin);
		createTables(admin);
		admin.close();
	}

	public static void saveInventoryTableData(InventoryDAO dao) throws IOException {
		System.out.println("------------------------------");
		System.out.println(" Inserting rows in Inventory Table: ");
		dao.addInventory("pens", 9);
		dao.addInventory("notepads", 21);
		dao.addInventory("erasers", 10);
	}

	private static void deleteTables(HBaseAdmin admin) throws IOException {
		InitTables.deleteTable(admin, ShoppingCartDAO.TABLE_NAME);
		InitTables.deleteTable(admin, InventoryDAO.TABLE_NAME);
	}

	private static void createTables(HBaseAdmin admin) throws IOException {
		InitTables.createTable(admin, InventoryDAO.TABLE_NAME, InventoryDAO.STOCK_CF, 3);
		InitTables.createTable(admin, ShoppingCartDAO.TABLE_NAME, ShoppingCartDAO.ITEMS_CF, 3);
	}

	public static void addInventory(InventoryDAO dao, String items, String quantity) throws IOException {
		dao.addInventory(items, Long.parseLong(quantity));
	}

	public static void deleteUserCart(ShoppingCartDAO dao, String user) throws IOException {
		printShoppingcartTable(dao);
		System.out.println("Delete Cart for " + user);
		dao.deleteShoppingCart(user);
		printShoppingcartTable(dao);
	}

	public static void saveShoppingCartData(ShoppingCartDAO dao) throws IOException {
		dao.addShoppingCart("Mike", 1, 2, 3);
		dao.addShoppingCart("Mary", 1, 2, 5);
		dao.addShoppingCart("Adam", 5, 4, 2);
	}

	public static void printInventoryTable(InventoryDAO dao) throws IOException {
		System.out.println("Get Inventory From Inventory Table");
		List<Inventory> list = dao.getInventorys();
		System.out.println("*****************************************************");
		System.out.println("print Inventorys from Table ...");

		for (Inventory inventory : list) {
			System.out.println(inventory);
		}
	}

	public static void printShoppingcartTable(ShoppingCartDAO dao) throws IOException {
		System.out.println("Scan Shopping Cart Table");
		List<ShoppingCart> list = dao.getShoppingCarts();
		System.out.println("*****************************************************");
		if (list.isEmpty()) {
			System.out.println("Table is empty");
		} 
		else {
			System.out.println("print Shoppingcart Table");

			for (ShoppingCart cart : list) {
				System.out.println(cart);
			}
		}
	}
}
