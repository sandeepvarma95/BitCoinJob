package org.stark.bitcoin;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class bitCoinJob extends TimerTask{

	public static Connection ConnectToDB() throws Exception {

		try {
			/*String driver = bitCoinConstants.MYSQLDRIVER;
			String url = bitCoinConstants.MYSQLURL;
			String username = bitCoinConstants.USERNAME;
			String password = bitCoinConstants.PASSWORD;
			Class.forName(driver);

			Connection conn = DriverManager.getConnection(url, username, password);*/
			
			String AWSurl = bitCoinConstants.AWSURL;
			String AWSusername = bitCoinConstants.AWSUSERNAME;
			String AWSpassword = bitCoinConstants.AWSPASSWORD;

			Connection conn = DriverManager.getConnection(AWSurl, AWSusername, AWSpassword);
			
			System.out.println("Database Connection Established");
			return conn;
		} catch (Exception e) {
			System.out.println(e);
		}

		return null;
	}

	@SuppressWarnings("deprecation")
	public void run() {

		long startTime = 0;
		
		System.out.println("Job created and running at "+ new Date());

		try {
			/*DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			Date today = Calendar.getInstance().getTime();*/
			
			Date today = new Date();

			String URL = bitCoinConstants.API_URL;

			URL url = new URL(URL);
			URLConnection req = url.openConnection();
			req.connect();
			JsonParser jp = new JsonParser();
			JsonElement root = jp.parse(new InputStreamReader((InputStream) req.getContent()));

			JsonObject rootobj = root.getAsJsonObject();

			JsonArray jarray = rootobj.getAsJsonArray("markets");

			Connection con = ConnectToDB();

			startTime = System.currentTimeMillis();

			/*
			 * PreparedStatement pstmtDel =
			 * con.prepareStatement(bitCoinConstants.TRUNCATE_QUERY);
			 * pstmtDel.execute();
			 */

			ResultSet rs;
			int lastId = 0;

			PreparedStatement psId = con.prepareStatement("SELECT last_column_id FROM bitcoin_metadata");
			rs = psId.executeQuery();

			while (rs.next()) {
				lastId = rs.getInt(1);
			}
			rs.close();
			psId.close();
			
			int x = lastId;

			
			  PreparedStatement pstmt = con.prepareStatement("INSERT INTO bitcoindata values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
			  
			  
			  for (int i = 0; i < jarray.size(); i++) 
			  { 
			  rootobj =jarray.get(i).getAsJsonObject(); 
			  String exchangeId = rootobj.get("exchange_id").toString(); 
			  String symbol = rootobj.get("symbol").toString(); 
			  String baseAsset = rootobj.get("base_asset").toString(); 
			  String quoteAsset = rootobj.get("quote_asset").toString(); 
			  String priceUnconverted = rootobj.get("price_unconverted").toString(); 
			  String price = rootobj.get("price").toString(); 
			  String change24H = rootobj.get("change_24h").toString(); 
			  String spread = rootobj.get("spread").toString(); 
			  String volume24H = rootobj.get("volume_24h").toString(); 
			  String status = rootobj.get("status").toString(); 
			  String createdDate = rootobj.get("created_at").toString(); 
			  String updatedDate = rootobj.get("updated_at").toString(); 
			  String systemLoadDate = today.toString();
			  
			  
			  pstmt.setInt(1, x); 
			  pstmt.setString(2, exchangeId);
			  pstmt.setString(3, symbol); 
			  pstmt.setString(4, baseAsset);
			  pstmt.setString(5, quoteAsset); 
			  pstmt.setString(6,priceUnconverted); 
			  pstmt.setString(7, price); 
			  pstmt.setString(8,change24H); 
			  pstmt.setString(9, spread); 
			  pstmt.setString(10,volume24H); 
			  pstmt.setString(11, status); 
			  pstmt.setString(12,createdDate); 
			  pstmt.setString(13, updatedDate);
			  pstmt.setString(14, systemLoadDate); 
			  pstmt.executeUpdate(); 
			  x++;
			  }
			  
		
	     PreparedStatement psNewId = con.prepareStatement("UPDATE bitcoin_metadata SET last_column_id=? WHERE id=?");
	     String systemLoadDate = new Date().toString();
	     //psNewId.setString(1,systemLoadDate);
	     psNewId.setInt(1,x);
	     psNewId.setInt(2, 1001);
	     psNewId.executeUpdate();
	     psNewId.close();
			 
		} catch (Exception e) {
			e.printStackTrace();
		}

		long endTime = System.currentTimeMillis();
		long totalTime = endTime - startTime;
		System.out.println("Total time in milliseconds to insert data: " + totalTime + " ms");

		System.out.println("All data inserted to database");
	}
}