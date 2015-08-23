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

import utility.SqlToolbox;

public class AdvnAmexSymbolScraper {
	
	public static void main(String[] args){
		WebDriver driver = null; // Outside of try catch so that I can close upon failure
		try {
			
			String url = "jdbc:mysql://localhost:3306/";
			String dbName = "Stocks";
			String userName = "root"; 
			String password = "";
			Connection connection = DriverManager.getConnection(url+dbName,userName,password);
			connection.setAutoCommit(false);
			connection.commit();
			Statement stmt = connection.createStatement();
			ResultSet rs =  stmt.executeQuery("select letter from NasdaqLetters order by letter");
			while(rs.next()){
				String letter = rs.getString("letter").toUpperCase();
				String webUrl = "http://www.advfn.com/amex/americanstockexchange.asp?companies="+letter;
				driver = new FirefoxDriver();
				driver.get(webUrl);
				Document document = Jsoup.parse(driver.getPageSource());
				Elements trs = document.select("b:containsOwn(Companies listed on the AMEX)")
						.get(0).parent().parent().parent().select("tr:has(td)");
				for(Element tr: trs){
					Elements tds = tr.select("td");
					Elements links = tds.get(0).select("a");
					if(links.size()>0){
						String company = tds.get(0).text().trim();
						String stock = tds.get(1).text().trim();
						Hashtable<String,String> record = new Hashtable<String,String>();
						record.put("company",company);
						record.put("stock",stock);
						record.put("exchange","AMEX");
						SqlToolbox.storeData(connection, "Stocks", "AllStockSymbols", record);
						System.out.println("Successfully stored "+company+" "+stock);
					} else {
						continue; // Only the headers don't have links
					}
				}
				driver.quit();
			}
			
			
			System.out.println("Finished.");
		} catch (Exception e){
			e.printStackTrace();
			driver.quit();
		}
	}
	
	
}
