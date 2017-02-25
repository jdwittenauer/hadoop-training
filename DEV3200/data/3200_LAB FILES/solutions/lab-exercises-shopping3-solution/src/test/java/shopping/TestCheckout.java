package shopping;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.junit.Before;
import org.junit.Test;

import shopping.dao.InventoryDAO;
import shopping.dao.ShoppingCartDAO;
import shopping.model.Inventory;

public class TestCheckout {

	@Before
	public void setup() throws Exception {
	}

	// TODO uncomment test
	// TODO 1a finish checkout code in InventoryDAO
	@Test
	public void testCreateSaveInventoryData2() throws Exception {
		System.out.println(" Test Checkout ");
		// put data in the Inventory table
		HTableInterface invTable = MockHTable.create();
		InventoryDAO inventoryDAO = new InventoryDAO(invTable);
		// put data in Inventory table
		inventoryDAO.addInventory("pens", 9);
		inventoryDAO.addInventory("notepads", 21);
		inventoryDAO.addInventory("erasers", 10);
		// scan Inventory table
		List<Inventory> list = inventoryDAO.getInventorys();
		System.out.println("Print Inventory ");
		for (Inventory inv : list) {
			System.out.println(inv);
		}
		// put data in the ShoppingCart
		HTableInterface cartTable = MockHTable.create();
		ShoppingCartDAO shoppingCartDAO = new ShoppingCartDAO(cartTable);
		// put data in Shopping cart cartId, pens, notepads, erasers
		shoppingCartDAO.addShoppingCart("Mike", 1, 2, 3);
		// TODO 1a finish checkout code in InventoryDAO
		ShoppingCartApp.checkout(inventoryDAO, shoppingCartDAO, "Mike");
		list = inventoryDAO.getInventorys();
		System.out.println("Print Inventory ");
		for (Inventory inv : list) {
			System.out.println(inv);
		}
		// pens should be 9-1 = 8
		Inventory inventory = inventoryDAO.getInventory("pens");
		assertEquals(inventory.quantity, 8);
		// notepads should be 21-2 = 19
		inventory = inventoryDAO.getInventory("notepads");
		assertEquals(inventory.quantity, 19);
		// erasers should be 10-3=7
		inventory = inventoryDAO.getInventory("erasers");
		assertEquals(inventory.quantity, 7);
	}

}
