package schemadesign;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

public class LookupTrades {

	private final static DateFormat dateArgFormat = new SimpleDateFormat("yyyyMMdd");

	public static void main(String[] args) throws IOException {
		// args[0]: tall or flat
		// args[1]: symbol
		// args[2]: startdate
		// args[3]: stopdate
		
		// 1. Check for correct args. Parse args. 
		if (args.length < 2) { // quit if not enough args provided
			System.out.println("Usage: LookupTrades {tall | flat}  symbol [startdate[, stopdate]]");
			System.out.println("Format dates as YYYYMMDD.");
			System.out.println("Example: LookupTrades flat  CSCO 20131021 20131025");
			return;
		}
		
		if (!(args[0].equals("tall") || args[0].equals("flat"))) { // quit if schema type not specified
			System.out.println("Specify a schema type of 'tall' or 'flat'.");
			return;
		} 

		String symbol = args[1];


		Long startDate = dateArgFormat.parse("19710101", new ParsePosition(0)).getTime();
		Long stopDate = dateArgFormat.parse("29991231", new ParsePosition(0)).getTime();
		if (args.length == 2) {	// Start & stop dates not provided
			System.out.println("No start and stop dates specified. Retrieving all trades for " + symbol);
		} else if (args.length == 3) { // Only a start date is provided
			startDate = dateArgFormat.parse(args[2], new ParsePosition(0)).getTime(); 
			System.out.println("Retrieving trades starting from " + args[3] + " for " + symbol);
		} else { // Both start & stop dates are provided
			startDate = dateArgFormat.parse(args[2], new ParsePosition(0)).getTime(); 
			stopDate = dateArgFormat.parse(args[3], new ParsePosition(0)).getTime(); 
			System.out.println("Retrieving trades for " + symbol + " from " + args[2] + " to " + args[3]);
		}
		
		// 2. Open connection to HBaseConfiguration and instantiate DAO to access the table.
		Configuration conf = HBaseConfiguration.create();
		TradeDAO tradeDao = null;
		if (args[0].equals("tall")) {
			tradeDao = new TradeDAOTall(conf);
		} else if (args[0].equals("flat")) {
			tradeDao = new TradeDAOFlat(conf);
		} 
		System.out.println("Using DAO: " + tradeDao.getClass());

		// 3. Read a set of trades via the DAO.
		List<Trade> retrievedTradeSet = tradeDao.getTradesByDate(symbol, startDate, stopDate);

		// 4. Print the results and exit.
		printTrades(retrievedTradeSet);
		tradeDao.close();
		return;
	}
	public static void printTrades(List<Trade> trades) {
		System.out.println("Printing " + trades.size() + " trades.");
		for (Trade trade : trades) {
			System.out.println(trade);
		}
	
	}

}