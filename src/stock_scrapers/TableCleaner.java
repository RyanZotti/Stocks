package stock_scrapers;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;

public class TableCleaner {

	public static void main(String[] args){
		try {
			String url = "jdbc:mysql://localhost:3306/";
			String dbName = "Stocks";
			String userName = "root"; 
			String password = "";
			Connection connection = DriverManager.getConnection(url+dbName,userName,password);;
			connection.setAutoCommit(false);
			connection.commit();
			Statement stmt = connection.createStatement();
			ArrayList<String> tables = new ArrayList<String>();
			tables.add("Q_Activity_Ratios");
			tables.add("Q_Against_the_Industry_Ratios");
			tables.add("Q_Assets");
			tables.add("Q_Capital_Structure_Ratios");
			tables.add("Q_Efficiency_Ratios");
			tables.add("Q_Equity_And_Liabilities");
			tables.add("Q_Financing_Activities");
			tables.add("Q_Income_Statement");
			tables.add("Q_Income_Statement_YTD");
			tables.add("Q_Indicators");
			tables.add("Q_Investing_Activities");
			tables.add("Q_Liquidity_Ratios");
			tables.add("Q_Misc");
			tables.add("Q_Net_Cash_Flow");
			tables.add("Q_Normalized_Ratios");
			tables.add("Q_Operating_Activities");
			tables.add("Q_Profit_Margins");
			tables.add("Q_Profitability");
			tables.add("Q_Solvency_Ratios");
			for(String table : tables){
				stmt.executeUpdate("delete from "+table+" where exists (select * from unparseable_stocks as b where "+table+".stock = b.stock and "+table+".quarter_end_date = b.quarter_end_date and "+table+".company = b.company)");
				connection.commit();
				System.out.println(table);
			}
			System.out.println("Finished.");
		} catch (Exception e){
			e.printStackTrace();
		}
	}
	
}
