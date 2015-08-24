package misc;

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
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.firefox.FirefoxDriver;

public class ScrapeStockSymbols {

	public static int pageNumber = 1;
	public static String nasdaqLetter  = null;
	
	public static void main(String [] args){
		
		// Define things for DriverManager.getConnection
		String url = "jdbc:mysql://localhost:3306/";
		String dbName = "Stocks";
		String userName = "root"; 
		String password = "";
		Document document = null;
		WebDriver driver = null;
		
		try {
			
			Connection connection = DriverManager.getConnection(url+dbName,userName,password);;
			connection.setAutoCommit(false);
			String qryInsert = "insert into stocksymbols(company_name, stock_symbol, market_cap_mm, country, ipo_year, sector, Nasdaq_letter) values(?, ?, ?, ?, ?, ?, ?)";
			PreparedStatement psPush = connection.prepareStatement(qryInsert);
			connection.commit();
			Statement sGetLetters = connection.createStatement();
			ResultSet rsGetLetters = sGetLetters.executeQuery("select letter from NasdaqLetters as a where not exists (select * from  StockSymbols as b where a.letter = b.Nasdaq_letter) order by letter asc");
			while(rsGetLetters.next()){
				nasdaqLetter = rsGetLetters.getString("letter");
				String webUrl = "http://www.nasdaq.com/screening/companies-by-name.aspx?letter="+nasdaqLetter;
				driver = new FirefoxDriver();
				driver.get(webUrl);
				boolean nextIsClickable = true;
				while(nextIsClickable){
					Thread.sleep(3000L);
					document = Jsoup.parse(driver.getPageSource());
					Hashtable<String,Integer> colIndices = new Hashtable<String,Integer>();
					Elements headers = document.select("table#CompanylistResults").get(0).select("thead").get(0).select("th");
					for(int h = 0; h < headers.size(); h++){
						String columnName = headers.get(h).text().replaceAll("[^A-Za-z ]","").trim();
						colIndices.put(columnName,h);
					}
					Elements rows = document.select("table#CompanylistResults").get(0).select("tbody").get(0).select("tr");
					for (Element row : rows){
						Elements tds = row.select("td");
						if(tds.size() > 3){
							String companyName = tds.get(colIndices.get("Name")).text();
							String stockSymbol = tds.get(colIndices.get("Symbol")).text();
							String marketCap = tds.get(colIndices.get("Market Cap")).text();
							String country = tds.get(colIndices.get("Country")).text();
							String ipoYear = tds.get(colIndices.get("IPO Year")).text();
							String sector = tds.get(colIndices.get("Subsector")).text();
							double marketCapDouble = 0;
							if(marketCap.contains("M")){
								marketCap = marketCap.replaceAll("\\$","").replaceAll("M","");
								marketCapDouble = Double.parseDouble(marketCap);
							} else if(marketCap.contains("B")){
								marketCap = marketCap.replaceAll("\\$","").replaceAll("B","");
								marketCapDouble = Double.parseDouble(marketCap) * 1000;
							}
							psPush.setString(1, companyName);
							psPush.setString(2, stockSymbol);
							psPush.setDouble(3, marketCapDouble);
							psPush.setString(4, country);
							psPush.setString(5, ipoYear);
							psPush.setString(6, sector);
							psPush.setString(7, nasdaqLetter);
							psPush.executeUpdate();
							connection.commit();
							System.out.println(companyName + " " + stockSymbol + " " + marketCap + " " + country + " " + ipoYear + " " + sector + " " + sector);
							
						}
					}
					Elements nextLink = document.select("a#main_content_lb_NextPage");
					if(nextLink.size() > 0){
						//System.out.println("greater than 0");
						driver.findElement(By.id("main_content_lb_NextPage")).click();
						pageNumber++;
						//System.out.println("test");
					} else {
						nextIsClickable = false;
						driver.quit();
						//System.out.println("zero");
					}		
				}
			}
			System.out.println("Finished.");
		} catch (Exception e){
			System.out.println("Letter: " + nasdaqLetter + " Page Number: " + pageNumber);
			e.printStackTrace();
			driver.quit();
			System.exit(1);
		} finally {
			//driver.quit();
		}
		
	}
	
}
