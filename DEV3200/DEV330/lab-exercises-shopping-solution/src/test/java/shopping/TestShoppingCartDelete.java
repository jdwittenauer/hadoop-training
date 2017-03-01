package shopping;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.junit.Before;
import org.junit.Test;

import shopping.dao.ShoppingCartDAO;
import shopping.model.ShoppingCart;

public class TestShoppingCartDelete {

    @Before
    public void setup() throws Exception {
    }

    // TODO 3a uncomment unit test
    // run unit test , it will be red
    // finish code in ShoppingCartDAO
    // test should run green
    // observe output on console
  //  @Test
    public void testDelete() throws Exception {
	System.out.println("---------------------");
	System.out.println(" Test 1 ");
	HTableInterface table = MockHTable.create();
	ShoppingCartDAO shoppingCartDAO = new ShoppingCartDAO(table);
	// put data in Shopping cart call addShoppingCart(
	// String cartId, long pens, long notepads, long erasers)
	shoppingCartDAO.addShoppingCart("Mike", 1, 2, 3);
	// get data from Inventory table
	ShoppingCart cart = shoppingCartDAO.getShoppingCart("Mike");
	assertEquals(cart.erasers, 3);
	System.out.println("Print Cart ");
	System.out.println(cart);
	// TODO 3a finish code in deleteShoppingCart
	shoppingCartDAO.deleteShoppingCart("Mike");
	cart = shoppingCartDAO.getShoppingCart("Mike");
	assertEquals(null, cart);
    }

    // TODO 3b uncomment unit test
    // run unit test , it should be green
    // observe output on console
  //  @Test
    public void testDelete2() throws Exception {
	System.out.println("---------------------");
	System.out.println(" Test 2 ");
	HTableInterface table = MockHTable.create();
	ShoppingCartDAO dao = new ShoppingCartDAO(table);
	// put data in Inventory table
	dao.addShoppingCart("Mike", 1, 2, 3);
	ShoppingCartApp.deleteUserCart(dao, "Mike");
	
    }

}
