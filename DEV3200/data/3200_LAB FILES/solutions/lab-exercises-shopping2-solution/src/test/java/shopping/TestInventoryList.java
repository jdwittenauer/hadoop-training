package shopping;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.junit.Before;
import org.junit.Test;

import shopping.dao.InventoryDAO;
import shopping.model.Inventory;

public class TestInventoryList {

    @Before
    public void setup() throws Exception {
    }

    // TODO 1a finish code run unit test observe output on console
    @Test
    public void testCreateSaveInventoryData1() throws Exception {
	System.out.println(" Test 1 ");
	HTableInterface table = MockHTable.create();
	InventoryDAO dao = new InventoryDAO(table);

	List<Put> puts = new ArrayList<Put>();

	System.out.println("------------------------------");
	System.out.println(" Inserting rows in Inventory Table: ");
	puts.add(dao.mkPut("pens", 4));
	// TODO 1a add a put to the list for notepads
	puts.add(dao.mkPut("notepads", 21));
	puts.add(dao.mkPut("erasers", 10));
	// put data in Inventory table
	// TODO 1a finish code in putInventoryList(puts);
	dao.putInventoryList(puts);
	List<Inventory> list = dao.getInventorys();
	assertEquals(3, list.size());
	System.out.println(" Print results for Inventory Table: ");
	for (Inventory inv : list) {
	    System.out.println(inv);
	}

	
    }
    // TODO 1b finish code run unit test observe output on console
    @Test
    public void testCreateSaveInventoryData2() throws Exception {
	System.out.println(" Test 2");
	HTableInterface table = MockHTable.create();
	InventoryDAO dao = new InventoryDAO(table);
	List<Put> puts = new ArrayList<Put>();
	System.out.println("------------------------------");
	System.out.println(" Inserting rows in Inventory Table: ");
	puts.add(dao.mkPut("pens",  9));
	puts.add(dao.mkPut("notepads", 22));
	// TODO 1b add a put to the list for erasers
	puts.add(dao.mkPut("erasers", 11));
	// put data in Inventory table
	// TODO 1b finish code in putInventoryBatch(puts);
	dao.putInventoryBatch(puts);
	List<Inventory> list = dao.getInventorys();
 	assertEquals(list.size(), 3);
	System.out.println(" Print results for Inventory Table: ");
	for (Inventory inv : list) {
	    System.out.println(inv);
	}
	
    }
   
    
    
    // TODO 1c run unit test observe output on console
    @Test
    public void testCreateSaveInventoryData3() throws Exception {
	System.out.println(" Test 3 " );
	HTableInterface table = MockHTable.create();
	InventoryDAO inventoryDAO = new InventoryDAO(table);
	// put data in Inventory table
	ShoppingCartApp.initInventoryTableList(inventoryDAO);
    }

}
