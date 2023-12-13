package file_read_db_write;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

public class mvmn_cmmc_vstr_rsd {

	private static java.sql.Statement stmt;

	public static void main(String[] agrs) throws IOException, ClassNotFoundException, SQLException {
		//엑셀 파일 경로
		File dir = new File("D:\\이동통신 데이터(1차)\\방문자 거주지");
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
		    System.out.println("fileCnt========>"+fileCnt);
		    System.out.println("파일이름 : "+fileNm);
		    
		    String[] fileNmAry = fileNm.replace("D:\\이동통신 데이터(1차)\\방문자 거주지\\","").split("_");

		    List<List<String>> csvList = new ArrayList<List<String>>();
	        File csv = new File(files[i].toString());
	        BufferedReader br = null;
	        String line = "";
	        
	        String SQL = "INSERT INTO jtassbigdata.mvmn_cmmc_vstr_rsd " + 
	        		"(STD_YY_MM_DD, BSC_LCGV_NM, VSTR_WDAR_LCGV_NM, VSTR_BSC_LCGV_NM, WDAR_LCGV_VSTR_CNT, WDAR_LCGV_VSTR_RT, BSC_LCGV_VSTR_CNT, BSC_LCGV_VSTR_RT) VALUES(?,?,?,?,?,?,?,?)"; 
	        
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
	                	
	                	
	                	
	                	pstmt.setString(1, fileNmAry[1]);
	                    pstmt.setString(2, fileNmAry[0]);
	                    pstmt.setString(3, lineArr[0]);
	                    pstmt.setString(4, lineArr[1]);
	                    pstmt.setString(5, lineArr[2]);
	                    pstmt.setString(6, lineArr[3]);
	                    pstmt.setString(7, lineArr[4]);
	                    pstmt.setString(8, lineArr[5]);
	                	
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
		System.out.println("totCnt========> 총 : "+totCnt+"건 완료");
	}
	
}
