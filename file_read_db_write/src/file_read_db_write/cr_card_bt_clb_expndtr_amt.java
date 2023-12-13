package file_read_db_write;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class cr_card_bt_clb_expndtr_amt {

	private static java.sql.Statement stmt;

	public static void main(String[] agrs) throws IOException, ClassNotFoundException, SQLException {
		//엑셀 파일 경로
		File dir = new File("E:\\신용카드 데이터");
		File files[] = dir.listFiles();
 
 
		//파일 명 
		String fileNm = "";
		
		//쿼리 문
		String sql = "";
		
		//1번라인 제거
		int k=0;
		int totCnt = 0;
		int fileCnt = 0;
		
		//db 연결
		Connection conn = null;
        stmt = null;
        PreparedStatement pstmt = null; 
 
		for (int i = 0; i < files.length; i++) {
        
			 //년월 및 기초 지자체 명 찾기
		    fileNm =  files[i].toString();
		    
		    fileCnt++;
		    
		    String[] fileNmAry = fileNm.replace("E:\\신용카드 데이터\\","").split("_");

		    List<List<String>> csvList = new ArrayList<List<String>>();
	        File csv = new File(files[i].toString());
	        BufferedReader br = null;
	        String line = "";
	        
	        String SQL = "INSERT INTO jtassbigdata.cr_card_bt_clb_expndtr_amt " + 
	        		"(STD_YR_MM, BSC_LCGV_NM, `DIV`, UCL, MDFG, UCL_EXPNDTR_AMT, UCL_EXPNDTR_AMT_RATIO, MDFG_EXPNDTR_AMT, MDFG_EXPNDTR_AMT_RATIO) VALUES(?,?,?,?,?,?,?,?,?)"; 
	        k=0;
	        
	        try {
	        	// 1. 드라이버 로딩
	            Class.forName("org.mariadb.jdbc.Driver");
	            // 2. 연결하기
	            conn = DriverManager.getConnection("jdbc:mariadb://221.146.220.2:3306/jtassbigdata", "jtassbigdata", "jtassbigdata!@#123");
	            
	            
	            br = new BufferedReader(new InputStreamReader(new FileInputStream(csv),"euc-kr"));
	            
	            pstmt = conn.prepareStatement(SQL); 
	            while ((line = br.readLine()) != null) { // readLine()은 파일에서 개행된 한 줄의 데이터를 읽어온다.
	                List<String> aLine = new ArrayList<String>();
	                String[] lineArr = line.split(","); // 파일의 한 줄을 ,로 나누어 배열에 저장 후 리스트로 변환한다.
	                aLine = Arrays.asList(lineArr);
	                if (k!=0) {
	                	pstmt.setString(1, fileNmAry[2]);
	                    pstmt.setString(2, fileNmAry[0]);
	                    pstmt.setString(3, fileNmAry[1]);
	                    pstmt.setString(4, lineArr[0]);
	                    pstmt.setString(5, lineArr[1]);
	                    pstmt.setString(6, lineArr[2]);
	                    pstmt.setString(7, lineArr[3]);
	                    pstmt.setString(8, lineArr[4]);
	                    if (lineArr.length < 6) {
	                    	pstmt.setString(9, "0");
	                    }else {
	                    	pstmt.setString(9, lineArr[5]);
	                    }

	                    pstmt.addBatch();
	                	
	                    //int r = pstmt.executeUpdate(); 
	                    
	                	totCnt++;
	                	
	                } else {
	                	k+=1;
	                }
	                
	            }
	            pstmt.executeBatch();
	        } catch (FileNotFoundException e) {
	            e.printStackTrace();
	        } catch (IOException e) {
	            e.printStackTrace();
	        } finally {
	            try {
	                if (br != null) { 
	                    br.close(); // 사용 후 BufferedReader를 닫아준다.
	                }
	                if( conn != null && !conn.isClosed()){
	                    conn.close();
	                }
	                if( stmt != null && !stmt.isClosed()){
	                    stmt.close();
	                }
	            } catch(IOException e) {
	                e.printStackTrace();
	            }
	        }
		    
		}
	}
	
}
