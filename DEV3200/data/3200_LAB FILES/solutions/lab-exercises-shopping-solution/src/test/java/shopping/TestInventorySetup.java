package shopping;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.junit.Before;
import org.junit.Test;

import shopping.dao.InventoryDAO;
import shopping.model.Inventory;

public class TestInventorySetup {

	@Before
	public void setup() throws Exception {
	}

	// TODO 1 run unit test observe output on console
	//@Test
	public void testCreateSaveInventoryData1() throws Exception {
		System.out.println(" Test 1 ");
		HTableInterface table = MockHTable.create();
		InventoryDAO inventoryDAO = new InventoryDAO(table);
		// put data in Inventory table
		inventoryDAO.addInventory("pens", 9);
		// get data from Inventory table
		Inventory inventory = inventoryDAO.getInventory("pens");
		assertEquals(inventory.quantity, 9);
		System.out.println("Print Inventory ");
		System.out.println(inventory);
	}

	// TODO 1 run unit test observe output on console
//@Test
	public void testCreateSaveInventoryData2() throws Exception {
		System.out.println(" Test 2 ");
		HTableInterface table = MockHTable.create();
		InventoryDAO inventoryDAO = new InventoryDAO(table);
		// put data in Inventory table
		inventoryDAO.addInventory("pens", 9);
		inventoryDAO.addInventory("notepads", 21);
		inventoryDAO.addInventory("erasers", 10);
		// scan Inventory table
		List<Inventory> list = inventoryDAO.getInventorys();
		assertEquals(list.size(), 3);
		for (Inventory inv : list) {
			System.out.println(inv);
		}
	}

	// TODO 1 run unit test observe output on console
//	@Test
	public void testCreateSaveInventoryData3() throws Exception {
		System.out.println(" Test 3 ");
		HTableInterface table = MockHTable.create();
		InventoryDAO inventoryDAO = new InventoryDAO(table);
		// put data in Inventory table
		ShoppingCartApp.saveInventoryTableData(inventoryDAO);
		// print data in Inventory table
		ShoppingCartApp.printInventoryTable(inventoryDAO);
	}

}
