package shopping.model;

import org.apache.hadoop.hbase.util.Bytes;


public class Inventory {
    public String stock;
    public long quantity;

    public Inventory(byte[] stock, byte[] quantity) {
	    this(Bytes.toString(stock), Bytes.toLong(quantity));
    }

    public Inventory(String stock, long quantity) {
        this.stock = stock;
        this.quantity = quantity;
    }

    @Override
    public String toString() {
	    return "Inventory [key=" + stock + ", quantity=" + quantity + "]";
    }
}
