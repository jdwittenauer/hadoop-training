package shopping;

import java.io.IOException;
import java.util.List;
import org.apache.log4j.Logger;
import shopping.dao.ShoppingCartDAO;
import shopping.model.ShoppingCart;


public class ShoppingCartTool {
	private static final Logger log = Logger.getLogger(ShoppingCartTool.class);

	public static void list(ShoppingCartDAO dao, String cartId) throws IOException {
		List<ShoppingCart> carts = dao.getShoppingCarts();
		log.info(String.format("Found %s carts.", carts.size()));
		for (ShoppingCart c : carts) {
			System.out.println(c);
		}
	}

	public static void addCart(ShoppingCartDAO dao, String cartId, long pens, long notepads, long erasers) throws IOException {
		log.debug("Adding cart...");
		dao.addShoppingCart(cartId, pens, notepads, erasers);
		ShoppingCart c = dao.getShoppingCart(cartId);
		System.out.println("Successfully added cart " + c);
	}

	public static void getCart(ShoppingCartDAO dao, String cartId) throws IOException {
		log.debug(String.format("Getting cart %s", cartId));
		ShoppingCart u = dao.getShoppingCart(cartId);
		System.out.println(u);
	}
}
