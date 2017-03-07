package shopping.model;

import org.apache.hadoop.hbase.util.Bytes;


public class ShoppingCart {
    public String cartId;
    public long pens;
    public long notepads;
    public long erasers;

    public ShoppingCart(String cartId, long pens, long notepads, long erasers) {
        this.cartId = cartId;
        this.pens = pens;
        this.notepads = notepads;
        this.erasers = erasers;
    }

    public ShoppingCart(byte[] cartId, byte[] pens, byte[] notepads, byte[] erasers) {
	    this(Bytes.toString(cartId), Bytes.toLong(pens), Bytes.toLong(notepads), Bytes.toLong(erasers));
    }

    @Override
    public String toString() {
	    return "ShoppingCart [rowkey=" + cartId + ", pens=" + pens + ", notepads=" + notepads + ", erasers=" + erasers + "]";
    }
}
