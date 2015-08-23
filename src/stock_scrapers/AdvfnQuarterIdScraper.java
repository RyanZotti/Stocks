package stock_scrapers;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Hashtable;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.firefox.FirefoxDriver;

//http://stackoverflow.com/questions/10553677/eclipse-how-to-give-dependency-between-projects
import utility.SqlToolbox;

public class AdvfnQuarterIdScraper {

	public static void main(String [] args){
		WebDriver driver = null; // Outside of try catch so that I can close upon failure
		String url = "jdbc:mysql://localhost:3306/";
		String dbName = "Stocks";
		String userName = "root"; 
		String password = "";
		try {
			//String company = "Borders";
			//String stock = "BGP";
			//String webUrl = "http://www.advfn.com/stock-market/NYSE/BGP/financials?btn=istart_date&istart_date=61&mode=quarterly_reports";
			Connection connection = DriverManager.getConnection(url+dbName,userName,password);
			connection.setAutoCommit(false);
			connection.commit();
			Statement sUrlSettings = connection.createStatement();
			ResultSet rsUrlSettings = sUrlSettings.executeQuery("select company_name, stock_symbol from StockSymbols as a where not exists (select * from AdvfnQuarterIds as b where a.stock_symbol = b.stock) and not exists (select * from ADVFN_messed_up_stocks as c where a.stock_symbol = c.stock) order by stock_symbol asc;");
			while(rsUrlSettings.next()){
				String stock = rsUrlSettings.getString("stock_symbol");
				String company = rsUrlSettings.getString("company_name");
				//String webUrl = "http://www.advfn.com/stock-market/NYSE/"+stock+"/financials";
				String webUrl = "http://www.advfn.com/stock-market/NYSE/"+stock+"/financials?btn=istart_date&istart_date=1&mode=quarterly_reports";
				driver = new FirefoxDriver();
				driver.get(webUrl);
				//driver.findElement(By.id("symbol_entry")).sendKeys("").click();
				Document document = Jsoup.parse(driver.getPageSource());
				driver.quit();
				Elements quarters = null;
				try {
					quarters = document.select("select#istart_dateid").get(0).select("option");
				} catch (Exception e){ // Some pages are messed up
					Hashtable<String,String> messedUpStock = new Hashtable<String,String>();
					messedUpStock.put("stock", stock);
					SqlToolbox.storeData(connection, "Stocks", "ADVFN_messed_up_stocks",messedUpStock);
					continue; // Skip the messed up stock
				}
				for(Element quarter : quarters){
					Hashtable<String,String> storableData = new Hashtable<String,String>();
					String quarterDate = quarter.text().trim();
					String quarterIndex = quarter.attr("value");
					storableData.put("company",company);
					storableData.put("stock",stock);
					storableData.put("quarterDate",quarterDate);
					storableData.put("quarterIndex", quarterIndex);
					SqlToolbox.storeData(connection, "Stocks", "AdvfnQuarterIds", storableData);
				}
				System.out.println("Scaped "+stock+" "+company);
			}
		} catch (Exception e){
			e.printStackTrace();
			driver.quit();
			System.exit(1);
		}
		System.out.println("Finished.");
	}
	
}
