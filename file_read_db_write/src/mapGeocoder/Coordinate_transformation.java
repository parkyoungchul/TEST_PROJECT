package mapGeocoder;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class Coordinate_transformation {
	 
	private static java.sql.Statement stmt;
	private static String GEOCODE_URL="http://dapi.kakao.com/v2/local/search/address.json?query=";
	private static String GEOCODE_USER_INFO="22f03b1ad502fefe43b847b6254e7829"; 

	public static void main(String[] agrs) throws IOException, ClassNotFoundException, SQLException {
		 
				
		        URL obj;
			
		        try{
		            //인코딩한 String을 넘겨야 원하는 데이터를 받을 수 있다.
		            String address = URLEncoder.encode("대구광역시 중구 동성로2가 동성로2길 81", "UTF-8");
		            
		            obj = new URL(GEOCODE_URL+address);
					
		            HttpURLConnection con = (HttpURLConnection)obj.openConnection();
		            
		            //get으로 받아오면 된다. 자세한 사항은 카카오개발자센터에 나와있다.
		            con.setRequestMethod("GET");
		            con.setRequestProperty("Authorization",GEOCODE_USER_INFO);
		            con.setRequestProperty("content-type", "application/json");
		            con.setDoOutput(true);
		            con.setUseCaches(false);
		            con.setDefaultUseCaches(false);
					
		            Charset charset = Charset.forName("UTF-8");
		            BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream(), charset));
		            
		            String inputLine;
		            StringBuffer response = new StringBuffer();
		            
		            while ((inputLine = in.readLine()) != null) {
		                response.append(inputLine);
		            }
					
		            //response 객체를 출력해보자
		            System.out.println(response.toString());
					
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			 
 
	}
	
}
