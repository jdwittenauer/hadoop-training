package filter;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Currency;
import java.util.Date;
import java.util.Locale;


public class Trade {
	private final Long tradeTime;
	private final String tradeSymbol; 
	private final Float tradePrice;
	private final Long tradeVolume;
	private final static DateFormat dateFormat = new SimpleDateFormat("yyyy.MM.dd HH:mm:ss");
	private final static Locale LOCALE = Locale.US;
	private final static Currency CURRENCY = Currency.getInstance(LOCALE);
	

	public Trade(String tradeSymbol, Float tradePrice, Long tradeVolume, Long tradeTime) {
		this.tradeTime = tradeTime;
		this.tradeSymbol = tradeSymbol; 
		this.tradePrice = tradePrice;
		this.tradeVolume = tradeVolume;
	}

	public Trade(String tradeSymbol, Float tradePrice, Long tradeVolume, String tradeTime) throws Exception {
		this.tradeTime = TimeUtil.convertStringToEpoch(tradeTime);
		this.tradeSymbol = tradeSymbol; 
		this.tradePrice = tradePrice;
		this.tradeVolume = tradeVolume;
	}

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
		return tradeSymbol + ": " + tradeVolume + " shares at " + CURRENCY.getSymbol() + 
			String.format("%.2f", tradePrice) + " at " + dateFormat.format(tradeTime);  
	}
}
