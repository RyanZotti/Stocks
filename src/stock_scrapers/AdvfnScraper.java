package stock_scrapers;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Hashtable;
import java.util.Set;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.firefox.FirefoxDriver;

//http://stackoverflow.com/questions/10553677/eclipse-how-to-give-dependency-between-projects
import utility.SqlToolbox;

public class AdvfnScraper {
	public static void main(String [] args){
		WebDriver driver = null; // Outside of try catch so that I can close upon failure
		String url = "jdbc:mysql://localhost:3306/";
		String dbName = "Stocks";
		String userName = "root"; 
		String password = "";
		try {
			Connection connection = DriverManager.getConnection(url+dbName,userName,password);;
			connection.setAutoCommit(false);
			connection.commit();
			Statement sUrlSettings = connection.createStatement();
			ResultSet rsUrlSettings = sUrlSettings.executeQuery("select company, stock, quarterIndex from AdvfnQuarterIds as a where quarterIndex != 0 and not exists (select * from Q_Liquidity_Ratios as b where a.quarterDate = b.quarter_end_date)");
			while(rsUrlSettings.next()){
				String exchange = "NYSE";
				String stock = rsUrlSettings.getString("stock");
				String company = rsUrlSettings.getString("company");
				String quarterIndex = rsUrlSettings.getString("quarterIndex");
				// String webUrl = "http://www.advfn.com/stock-market/NYSE/BGP/financials?btn=istart_date&istart_date=61&mode=quarterly_reports";
				String webUrl = "http://www.advfn.com/stock-market/"+exchange+"/"+stock+"/financials?btn=istart_date&istart_date="+quarterIndex+"&mode=quarterly_reports";
				driver = new FirefoxDriver();
				driver.get(webUrl);
 
				Document document = Jsoup.parse(driver.getPageSource());
				driver.quit();
				Hashtable<String,Hashtable<String,String>> storableData = new Hashtable<String,Hashtable<String,String>>();
				Elements trs = document.select("font:containsOwn(INDICATORS)")
						.get(0).parent().parent().parent().select("tr");
				String sectionText = null;
				for (Element tr : trs){
					Elements tds = tr.select("td");
					if(tds.size() == 1) {
						sectionText = tds.get(0).text().replace("*","").trim();
						if(sectionText.length() > 0){
							if(!storableData.containsKey(sectionText)){
								storableData.put(sectionText,new Hashtable<String,String>());
							}
						} else {
							sectionText = "misc";
							if(!storableData.containsKey(sectionText)){
								storableData.put(sectionText,new Hashtable<String,String>());
							}	
						}
					} else if(tds.size() > 1) {
						String field = tds.get(0).text() // Remove chars that wouldn't work as MySQL cols
								.trim().replace("(","").replace(")","").replace(" ","_")
								.replace("%","pct").replace("-", "_").replace("&","and")
								.replace("/", "_over_").replace(",","").replace("$","")
								.replace(".", "").toLowerCase();
						String value = tds.get(1).text().trim().replace(",","");
						storableData.get(sectionText).put(field, value);
					}
				}
				Set<String> sections = storableData.keySet();
				for(String section : sections){
					storableData.get(section).put("company", company); // composite primary key
					storableData.get(section).put("stock", stock); // composite primary key
					String quarter_end_date = storableData.get("INDICATORS").get("quarter_end_date");
					storableData.get(section).put("quarter_end_date",quarter_end_date); // composite primary key
					/*
					Set<String> fields = storableData.get(section).keySet();
					for(String field : fields){
						System.out.println(section+" "+field+" "+storableData.get(section).get(field));
					}
					*/
				}
				SqlToolbox.storeData(connection, "Stocks", "Q_Indicators", storableData.get("INDICATORS"));
				SqlToolbox.storeData(connection, "Stocks", "Q_Capital_Structure_Ratios", storableData.get("CAPITAL STRUCTURE RATIOS"));
				SqlToolbox.storeData(connection, "Stocks", "Q_Equity_And_Liabilities", storableData.get("EQUITY & LIABILITIES"));
				SqlToolbox.storeData(connection, "Stocks", "Q_Profit_Margins", storableData.get("PROFIT MARGINS"));
				SqlToolbox.storeData(connection, "Stocks", "Q_Net_Cash_Flow", storableData.get("NET CASH FLOW"));
				SqlToolbox.storeData(connection, "Stocks", "Q_Investing_Activities", storableData.get("INVESTING ACTIVITIES"));
				SqlToolbox.storeData(connection, "Stocks", "Q_Profitability", storableData.get("PROFITABILITY"));
				SqlToolbox.storeData(connection, "Stocks", "Q_Income_Statement", storableData.get("INCOME STATEMENT"));
				SqlToolbox.storeData(connection, "Stocks", "Q_Normalized_Ratios", storableData.get("NORMALIZED RATIOS"));
				SqlToolbox.storeData(connection, "Stocks", "Q_Financing_Activities", storableData.get("FINANCING ACTIVITIES"));
				SqlToolbox.storeData(connection, "Stocks", "Q_Misc", storableData.get("misc"));
				SqlToolbox.storeData(connection, "Stocks", "Q_Activity_Ratios", storableData.get("ACTIVITY RATIOS"));
				SqlToolbox.storeData(connection, "Stocks", "Q_Efficiency_Ratios", storableData.get("EFFICIENCY RATIOS"));
				SqlToolbox.storeData(connection, "Stocks", "Q_Against_the_Industry_Ratios", storableData.get("AGAINST THE INDUSTRY RATIOS"));
				SqlToolbox.storeData(connection, "Stocks", "Q_Solvency_Ratios", storableData.get("SOLVENCY RATIOS"));
				SqlToolbox.storeData(connection, "Stocks", "Q_Operating_Activities", storableData.get("OPERATING ACTIVITIES"));
				SqlToolbox.storeData(connection, "Stocks", "Q_Assets", storableData.get("ASSETS"));
				SqlToolbox.storeData(connection, "Stocks", "Q_Income_Statement_YTD", storableData.get("INCOME STATEMENT (YEAR-TO-DATE)"));
				SqlToolbox.storeData(connection, "Stocks", "Q_Liquidity_Ratios", storableData.get("LIQUIDITY RATIOS"));	
				System.out.println("Successfully scraped data for "+stock+" "+company+" "+quarterIndex);
			}		
		} catch (Exception e) {
			//driver.quit();
			e.printStackTrace();
			System.exit(1);
		}
		System.out.println("Finished.");	
	}
}
