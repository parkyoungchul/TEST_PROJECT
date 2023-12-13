package morphological_analysis;

import org.snu.ids.kkma.index.Keyword;
import org.snu.ids.kkma.index.KeywordExtractor;
import org.snu.ids.kkma.index.KeywordList;
import org.snu.ids.kkma.ma.MExpression;
import org.snu.ids.kkma.ma.MorphemeAnalyzer;
import org.snu.ids.kkma.ma.Sentence;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

	
	public class kkma {
		private static java.sql.Statement stmt;
		
	    public static void main(String[] args) throws SQLException{
	    maTest();
	    //extractTest();
	}

    public static void maTest() throws SQLException
    {
       // String string = "꼬꼬마분석기를돌려보겟습니다";
        
        //db 연결
        Connection conn = null; 
        Statement  Sstmt = null; 
        ResultSet  rs = null; 
        
        PreparedStatement pstmt = null; 
        
        String[] result1;
        String[] result2;
        int totCnt = 0;
              
        String sql = "SELECT * FROM jtassbigdata_bak.extrl_news_data_colct where  (MORPSEME is null OR MORPSEME='') limit 0,10000";  
        String upDataQuery = "update jtassbigdata_bak.extrl_news_data_colct set MORPSEME = ? where OPERT_ID = ? ";
        
        try {
        	
        	String OPERT_ID ="";
        	String string = "";
        	
        	Pattern pattern = Pattern.compile("[/](.*?)[/]"); 
        	Matcher matcher;
        	
        	String morphological = "";


        	
            // 1. 드라이버 로딩
            Class.forName("org.mariadb.jdbc.Driver");
            // 2. 연결하기
            conn = DriverManager.getConnection("jdbc:mariadb://221.146.220.2:3306/jtassbigdata_bak", "jtassbigdata", "jtassbigdata!@#123");
           stmt = conn.createStatement(); 
           
           rs = stmt.executeQuery(sql);
           
           System.out.println("rs.length ==>" + rs.getRow());
           MorphemeAnalyzer ma = new MorphemeAnalyzer();
           while (rs.next()) {
        	   OPERT_ID = rs.getString(1);
        	   
        	   System.out.println("OPERT_ID = " +rs.getString("OPERT_ID"));
        	   string =rs.getString("SJ")+rs.getString("CN");
        	   
        	   
               ma.createLogger(null);
               List<MExpression> ret = ma.analyze(string);
               ret = ma.postProcess(ret);
               ret = ma.leaveJustBest(ret);
               List<Sentence> stl = ma.divideToSentences(ret);
               for( int i = 0; i < stl.size(); i++ ) {
                   Sentence st = stl.get(i);
                 //  System.out.println("=============================================  " + st.getSentence());
                   for( int j = 0; j < st.size(); j++ ) {
                      // System.out.println(st.get(j).toString().replace("[","").replace("]",""));
                       // 분석된 형태소를 "+" 값으로 분류
                       result1 =  st.get(j).toString().replace("[","").replace("]","").split("\\+");
                       
                       for( int k = 0; k < result1.length; k++ ) {
                    	   //"+"로 분리된 값 중에서 외국어, 보통명사, 고유 명사 찾기
                    	   if(result1[k].contains("OL") || result1[k].contains("NNG")  || result1[k].contains("NNP")) {
                    		   
                    		  // 찾은 외구어, 보통 명사, 고유명사에서 값만 분리하기
                    		   matcher = pattern.matcher(result1[k]); 
                    		   while (matcher.find()) {  // 일치하는 게 있다면   
                    			   //형태소 중복 제저
                    			   if (!morphological.contains(matcher.group(1)+",") && morphological.length() < 3900){ 
                    				   if (matcher.group(1).length() >2) {
                    					   
                    					   if (!morphological.contains(matcher.group(1))) {
                    						   morphological +="'"+matcher.group(1)+"'"+"," ;
                    					   }
                    				   }
	                    			    
	                    			    if(matcher.group(1) ==  null)
	                    			    	break;
                    				   
                    			   }
                    			} 
                    	   }
                       }
                   }
               } 
               
               System.out.println("===========OPERT_ID==================================  " + OPERT_ID);
               
               pstmt = conn.prepareStatement(upDataQuery); 
               System.out.println("===========morphological.length()==================================  " + morphological.length()+"  >>>>> " + morphological);
               if (morphological !=null && morphological.length() >2) {
	               pstmt.setString(1, "{"+morphological.substring(0, morphological.length()-1)+"}"); 
	               pstmt.setString(2, OPERT_ID);
	           	
	               int r = pstmt.executeUpdate();
               }
               
               //형태소 초기화
               morphological = "";
               totCnt++;
               
              
               
           }
           System.out.println("===========totCnt==================================  " + totCnt);
           ma.closeLogger();
            
            
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            if( rs != null){
			    rs.close();
			}
			
			if( stmt != null && !stmt.isClosed()){
			    stmt.close();
			}
        
			if( conn != null && !conn.isClosed()){
			    conn.close();
			}
        }
    }
    
    public static void extractTest() throws SQLException{
// String string = "꼬꼬마분석기를돌려보겟습니다";
        
        //db 연결
        Connection conn = null; 
        Statement  pstmt = null; 
        ResultSet  rs = null; 
        
        String[] result1;
        String[] result2;
              
        String sql = "SELECT * FROM jtassbigdata.extrl_news_data_colct limit 0,1";      
        
        try {
        	
        	String OPERT_ID ="";
        	String string = "";
        	
        	Pattern pattern = Pattern.compile("[/](.*?)[/]"); 
        	Matcher matcher;
        	
        	String morphological = "";


        	
            // 1. 드라이버 로딩
            Class.forName("org.mariadb.jdbc.Driver");
            // 2. 연결하기
            conn = DriverManager.getConnection("jdbc:mariadb://221.146.220.2:3306/jtassbigdata", "jtassbigdata", "jtassbigdata!@#123");
           pstmt = conn.createStatement(); 
           
           rs = pstmt.executeQuery(sql);
           
           System.out.println("rs.length ==>" + rs.getRow());
           MorphemeAnalyzer ma = new MorphemeAnalyzer();
           while (rs.next()) {
               string = rs.getString("SJ") + rs.getString("CN");;
               KeywordExtractor ke = new KeywordExtractor();
               KeywordList kl = ke.extractKeyword(string, true);
               for( int i = 0; i < kl.size(); i++ ){
                   Keyword kwrd = kl.get(i);
                   System.out.println(kwrd.getString() + "\t" + kwrd.getCnt());
               }
           }
           
           ma.closeLogger();
            
            
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            if( rs != null){
			    rs.close();
			}
			
			if( stmt != null && !stmt.isClosed()){
			    stmt.close();
			}
        
			if( conn != null && !conn.isClosed()){
			    conn.close();
			}
        }
    }
}

/**





    

    
}

**/
