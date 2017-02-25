package schemadesign;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.Currency;

/**
 * The Trade class represents trade events on a stock exchange.
 * A Trade instance is defined by date, ticker symbol, price and volume. 
 */
public class Trade {
	private final Long tradeTime;
	private final String tradeSymbol; 
	private final Float tradePrice;
	private final Long tradeVolume;

	// Constants for a locale 
	private final static DateFormat dateFormat = new SimpleDateFormat("yyyy.MM.dd HH:mm:ss");
	private final static Locale LOCALE = Locale.US;
	private final static Currency CURRENCY = Currency.getInstance(LOCALE);
	
	/**
	 * constructor for a Trade object
	 * This implementation explicitly sets the tradeTime to a given value. 
	 * @param tradeDate timestamp of the trade event
	 * @param tradeSymbol stock symbol, ex: "GOOG"
	 * @param tradePrice per-share price 
	 * @param tradeVolume number of shares transacted
	 */
	public Trade(String tradeSymbol, Float tradePrice, Long tradeVolume, Long tradeTime) {
		this.tradeTime = tradeTime;
		this.tradeSymbol = tradeSymbol; 
		this.tradePrice = tradePrice;
		this.tradeVolume = tradeVolume;
	}

	/**
	 * constructor for a Trade object 
	 * This implementation sets the tradeTime to the current time. 
	 * @param tradeSymbol stock symbol, ex: "GOOG"
	 * @param tradePrice per-share price 
	 * @param tradeVolume number of shares transacted
	 */
	public Trade(String tradeSymbol, Float tradePrice, Long tradeVolume) {
		this.tradeTime = new Date().getTime();
		this.tradeSymbol = tradeSymbol; 
		this.tradePrice = tradePrice;
		this.tradeVolume = tradeVolume;
	}

	public Long getTime() {
		return tradeTime;
	}
	
	public String getSymbol() {
		return tradeSymbol;
	}
	
	public Float getPrice() {
		return tradePrice;
	}
	
	public Long getVolume() {
		return tradeVolume;
	}
	
	@Override
	public String toString() {
		return tradeSymbol + ": " + tradeVolume + " shares at " + CURRENCY.getSymbol() + String.format("%.2f", tradePrice) + 
				" at " + dateFormat.format(tradeTime);  
	}
}
