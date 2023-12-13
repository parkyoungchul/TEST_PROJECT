package youtube;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Map;

public class youtube {
	private static java.sql.Statement stmt;
		
	public static void main(String[] args) throws IOException {
	    //maTest();
		
		java.util.ArrayList<java.util.Map<String, String>> temp2Array = getYouTubeSearch();
		
		for (Map<String, String> map : temp2Array) {
			System.out.println(map.get("title")+" :::: "+map.get("videoId"));
		}
	}
	
	public static java.util.ArrayList<java.util.Map<String, String>> getYouTubeSearch() {
		java.util.ArrayList<java.util.Map<String, String>> uRtnArray = new java.util.ArrayList<java.util.Map<String, String>>();
		
		try {
			
			String apiurl = "https://www.googleapis.com/youtube/v3/search";
			apiurl += "?key=AIzaSyBPMxP69hFC86ardFJXVnb5fZmkDyFIMtM";
			apiurl += "&part=snippet&type=video&maxResults=50&videoEmbeddable=true&publishedAfter=2022-01-02T00:00:00Z&publishedBefore=2022-02-01T23:59:59Z";
			apiurl += "&q="+URLEncoder.encode("성산일출봉","UTF-8");
			
			java.net.URL url = new java.net.URL(apiurl);
			java.net.HttpURLConnection conn = (java.net.HttpURLConnection) url.openConnection();
			conn.setDoOutput(true);
			conn.setRequestMethod("GET");
			conn.setRequestProperty("Content-Type", "application/json");
			conn.setRequestProperty("Accept-Charset", "UTF-8");
			conn.setConnectTimeout(3000);
			conn.setReadTimeout(3000);

			String inputLine = "";
			StringBuffer outResult = new StringBuffer();
			java.io.BufferedReader in = new java.io.BufferedReader(new java.io.InputStreamReader(conn.getInputStream(), "UTF-8"));
			while ((inputLine = in.readLine()) != null) {
				//System.out.println(inputLine);
				outResult.append(inputLine);
			}
			conn.disconnect();

			
			org.json.JSONObject youtubeJSON = new org.json.JSONObject(outResult.toString());
			if (youtubeJSON.getJSONArray("items").length() == 0)
				return uRtnArray;
			
			org.json.JSONArray items = youtubeJSON.getJSONArray("items");
			for (Object item : items) {
				org.json.JSONObject itemDtl = (org.json.JSONObject)item;
				
				java.util.Map<String, String> uRtn = new java.util.HashMap<String, String>();
				uRtn.put("title", itemDtl.getJSONObject("snippet").getString("title"));
				uRtn.put("channelTitle", itemDtl.getJSONObject("snippet").getString("channelTitle"));
				uRtn.put("channelId", itemDtl.getJSONObject("snippet").getString("channelId"));
				uRtn.put("description", itemDtl.getJSONObject("snippet").getString("description"));
				uRtn.put("videoId", itemDtl.getJSONObject("id").getString("videoId"));
				uRtn.put("thumbnails", itemDtl.getJSONObject("snippet").getJSONObject("thumbnails").getJSONObject("high").getString("url"));
				uRtn.put("publishedAt", itemDtl.getJSONObject("snippet").getString("publishedAt").substring(0, 10).replaceAll("[-]", "."));
				uRtnArray.add(uRtn);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return uRtnArray;
	}

    public static void maTest() throws IOException 
    {
    	String apiurl = "https://www.googleapis.com/youtube/v3/search";
		apiurl += "?key=AIzaSyBPMxP69hFC86ardFJXVnb5fZmkDyFIMtM";
		apiurl += "&part=snippet&type=video&maxResults=100&videoEmbeddable=true&publishedAfter=2022-01-02T00:00:00Z&publishedBefore=2022-02-01T23:59:59Z";
		apiurl += "&q="+URLEncoder.encode("성산일출봉","UTF-8");
		
		URL url = new URL(apiurl);
		HttpURLConnection con = (HttpURLConnection) url.openConnection();
		con.setRequestMethod("GET");
			
		BufferedReader br = new BufferedReader(new InputStreamReader(con.getInputStream(),"UTF-8"));
		String inputLine;
		StringBuffer response = new StringBuffer();
		while((inputLine = br.readLine()) != null) {
			System.out.println(inputLine);
		}
		br.close();
		 
    }
    
}
 
