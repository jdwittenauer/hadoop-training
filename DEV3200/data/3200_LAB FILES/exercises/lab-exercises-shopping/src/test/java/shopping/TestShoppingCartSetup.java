package shopping;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.junit.Before;
import org.junit.Test;

import shopping.dao.InventoryDAO;
import shopping.dao.ShoppingCartDAO;
import shopping.model.Inventory;
import shopping.model.ShoppingCart;

public class TestShoppingCartSetup {

    @Before
    public void setup() throws Exception {
    }

    // TODO 2a uncomment unit test
    // run unit test , it will be red
    // finish code in ShoppingCartDAO
    // test should run green
    // observe output on console
    // @Test
    public void testCreateSaveData1() throws Exception {
	System.out.println(" Test 1 ");
	HTableInterface table = MockHTable.create();
	ShoppingCartDAO shoppingCartDAO = new ShoppingCartDAO(table);
	// put data in Shopping cart call addShoppingCart(
	// String cartId, long pens, long notepads, long erasers)
	// TODO 2a Complete the code in shoppingCartDAO.addShoppingCart()
	shoppingCartDAO.addShoppingCart("Mike", 1, 2, 3);
	// get data from Inventory table
	// TODO 2a Complete the code in shoppingCartDAO.getShoppingCart
	ShoppingCart cart = shoppingCartDAO.getShoppingCart("Mike");
	assertEquals(cart.erasers, 3);
	System.out.println("Print Cart ");
	System.out.println(cart);
    }

    // TODO 2b uncomment unit test
    // run unit test , it will be red
    // finish code in ShoppingCartDAO
    // test should run green
    // observe output on console
   // @Test
    public void testCreateSaveData2() throws Exception {
	System.out.println(" Test 2 ");
	HTableInterface table = MockHTable.create();
	ShoppingCartDAO shoppingCartDAO = new ShoppingCartDAO(table);
	// put data in ShoppingCart table
	shoppingCartDAO.addShoppingCart("Mike", 1, 2, 3);
	shoppingCartDAO.addShoppingCart("Mary", 1, 2, 5);
	shoppingCartDAO.addShoppingCart("Adam", 5, 4, 2);
	// scan ShoppingCart table
	// TODO 2b Complete the code in shoppingCartDAO.getShoppingCarts();
	List<ShoppingCart> list = shoppingCartDAO.getShoppingCarts();
	assertEquals(list.size(), 3);
	System.out.println("Print Cart ");
	for (ShoppingCart cart : list) {
	    System.out.println(cart);
	}
    }

    // TODO 2c uncomment unit test
    // run unit test , it should be green 
    // observe output on console
   // @Test
    public void testCreateSaveData3() throws Exception {
	System.out.println(" Test 3 ");
	HTableInterface table = MockHTable.create();
	ShoppingCartDAO dao = new ShoppingCartDAO(table);
	// put data in Inventory table
	ShoppingCartApp.saveShoppingCartData(dao);
	// print data in Inventory table
	ShoppingCartApp.printShoppingcartTable(dao);
    }

}
