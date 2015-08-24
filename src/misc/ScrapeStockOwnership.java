package misc;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Hashtable;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.firefox.FirefoxDriver;

public class ScrapeStockOwnership {

	public static void main(String [] ags){
		Document document = null;
		WebDriver driver = null;
		try {
			// Define things for DriverManager.getConnection
			String url = "jdbc:mysql://localhost:3306/";
			String dbName = "Stocks";
			String userName = "root"; 
			String password = "";
			Connection connection = DriverManager.getConnection(url+dbName,userName,password);;
			connection.setAutoCommit(false);
			Statement sGetStocks = connection.createStatement();
			ResultSet rsGetStocks = sGetStocks.executeQuery("select * from StockSymbols as a where not exists (select * from (select * from InstitutionalHolders as b group by stock_symbol) as b where a.stock_symbol = b.stock_symbol group by b.stock_symbol) and not exists (select * from MessedUpStocks as c where c.symbol = a.stock_symbol)");
			String qryInsert = "insert into institutionalholders(company_name, stock_symbol, institution, shares_held, position_value, pct_of_total_holdings, pct_owned_of_shares_outstanding) values(?, ?, ?, ?, ?, ?, ?)";
			PreparedStatement psPush = connection.prepareStatement(qryInsert);
			PreparedStatement psPushMessedup = connection.prepareStatement("insert into messedupstocks(Symbol) values(?)");
			connection.commit();
			while(rsGetStocks.next()){
				String stockSymbol = rsGetStocks.getString("stock_symbol");
				String companyName = rsGetStocks.getString("company_name");
				//String webUrl = "http://data.cnbc.com/quotes/"+stockSymbol+"/tab/8";
				String webUrl = "http://apps.cnbc.com/view.asp?country=US&uid=stocks/ownership&symbol="+stockSymbol;
				//driver = new FirefoxDriver();
				//driver.get(webUrl);
				document = Jsoup.connect(webUrl).get();
				//Thread.sleep(3000L);
				//document = Jsoup.parse(driver.getPageSource());
				Hashtable<String,Integer> colIndices = new Hashtable<String,Integer>();
				//Elements headers = document.select("tbody#tBody_institutions");//.get(0).parent().select("thead").get(0).select("th");
				Elements headers = null;
				try {
					headers = document.select("tbody#tBody_institutions").get(0).parent().select("thead").get(0).select("th");
				} catch (Exception e2){
					psPushMessedup.setString(1, stockSymbol);
					psPushMessedup.executeUpdate();
					connection.commit();
					continue;
				}
				
				//System.out.println(headers);
				
				//Elements headers = document.select("tbody#tBody_institutions");//.get(0).parent().select("thead").get(0).select("th");
				for(int h = 0; h < headers.size(); h++){
					String columnName = headers.get(h).select("a").get(0).text().trim();
					colIndices.put(columnName,h);
				}
				Elements rows = document.select("tbody#tBody_institutions").select("tr");
				for(Element row : rows){
					Elements tds = row.select("td");
					String institutionName = tds.get(colIndices.get("Name")).text().replaceAll("\\.\\.\\.","").trim();
					String sharesHeld = tds.get(colIndices.get("Shares Held")).text();
					String positionValue = tds.get(colIndices.get("Position Value")).text().replaceAll("\\$","").replaceAll(",","");
					//String positionValue = tds.get(colIndices.get("Position Value")).text();
					String pctOfTotalHoldings = tds.get(colIndices.get("Percentage of Total Holdings")).text().replaceAll("%","").replaceAll("\\+","");
					String pctOwnedOfSharesOutstanding = tds.get(colIndices.get("% Owned of Shares Outstanding")).text().replaceAll("%","").replaceAll("\\+","");
					
					//BigDecimal sharesHeldBD = null;
					int positionValueInt = 0;
					double pctOfTotalHoldingsDouble = 0;
					double pctOwnedOfSharesOutstandingDouble = 0;
					
					BigDecimal sharesHeldBD = new BigDecimal("0");
					BigDecimal thousand = new BigDecimal("1000");
					BigDecimal million = new BigDecimal("1000000");
					BigDecimal billion = new BigDecimal("1000000000");
					
					 
					if(sharesHeld.contains("M")){
						sharesHeldBD = new BigDecimal(sharesHeld.replaceAll("M", "").replaceAll("\\$","")).multiply(million);
					} else if(sharesHeld.contains("B")){
						sharesHeldBD = new BigDecimal(sharesHeld.replaceAll("B", "").replaceAll("\\$","")).multiply(billion);
					} else if(sharesHeld.contains("K")){
						sharesHeldBD = new BigDecimal(sharesHeld.replaceAll("K", "").replaceAll("\\$","")).multiply(thousand);
					} else {
						sharesHeldBD = new BigDecimal(sharesHeld.replaceAll("K", "").replaceAll("\\$",""));
					}
					
					//positionValueInt = Integer.parseInt(positionValue);
					pctOfTotalHoldingsDouble = Double.parseDouble(pctOfTotalHoldings);
					pctOwnedOfSharesOutstandingDouble = Double.parseDouble(pctOwnedOfSharesOutstanding.replaceAll("--","0"));
					
					psPush.setString(1,companyName);
					psPush.setString(2,stockSymbol);
					psPush.setString(3,institutionName);
					psPush.setString(4,sharesHeldBD.toString().replaceAll("\\.0", ""));
					psPush.setString(5,positionValue);
					psPush.setDouble(6,pctOfTotalHoldingsDouble);
					psPush.setDouble(7,pctOwnedOfSharesOutstandingDouble);
					psPush.executeUpdate();
					connection.commit();
					//System.out.println(institutionName + " " + sharesHeldDouble + " " + positionValueInt + " " + pctOfTotalHoldingsDouble + " " + pctOwnedOfSharesOutstandingDouble);
				}
			 System.out.println(companyName + " " + stockSymbol);
			}
			System.out.println("Finished.");
		} catch (Exception e){
			e.printStackTrace();
			//driver.quit();
			System.exit(1);
		}
	}
	
	
}
