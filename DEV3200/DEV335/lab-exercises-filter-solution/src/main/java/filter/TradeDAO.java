package filter;

import java.io.IOException;
import java.util.List;


public interface TradeDAO {
	public void store(Trade trade) throws IOException;
	public void close() throws IOException;
	public List<Trade> getTradesByDate(String symbol, Long from, Long to) throws IOException;
}
