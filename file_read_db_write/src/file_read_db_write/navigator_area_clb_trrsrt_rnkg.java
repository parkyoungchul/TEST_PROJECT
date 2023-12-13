package file_read_db_write;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

public class navigator_area_clb_trrsrt_rnkg {
	public static void main(String[] agrs) throws FileNotFoundException, ClassNotFoundException, SQLException, UnsupportedEncodingException {
		//엑셀 파일 경로
		File dir = new File("D:\\내비게이션데이터");
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
        PreparedStatement pstmt = null; 
        
        String SQL = "INSERT INTO jtassbigdata.navigator_area_clb_trrsrt_rnkg " + 
        		"(STD_YR_MM, RNKG, WDAR_LCGV, SGG, TRRSRT_NM, ROAD_NM_ADDR, MDFG_CTG, SMCL_CTG, SRH_CNT) VALUES(?,?,?,?,?,?,?,?,?)"; 
        // 1. 드라이버 로딩
        Class.forName("org.mariadb.jdbc.Driver");
        // 2. 연결하기
        conn = DriverManager.getConnection("jdbc:mariadb://221.146.220.2:3306/jtassbigdata", "jtassbigdata", "jtassbigdata!@#123");
        
        BufferedReader br = null;
		for (int i = 0; i < files.length; i++) {
			
			
		    System.out.println("-- file: " + files[i]);
		    //FileInputStream file = new FileInputStream(files[i]);
		    
		    //br = new BufferedReader(new FileReader(files[i]));
		    
		    br = new BufferedReader(new InputStreamReader(new FileInputStream(files[i] ),"euc-kr"));
		    
		    //년월 및 기초 지자체 명 찾기
		    fileNm =  files[i].toString();
		    String[] fileNmAry = fileNm.split("_");
		    
		    System.out.println(fileNmAry[1]);
		    
		    pstmt = conn.prepareStatement(SQL);
		    
	 
		    try {
		    	 
				String line = ""; 
				while((line=br.readLine()) != null) {
					if (k!=0) {
						String[] token = line.split(",");
						List<String> tempList = new ArrayList<String>(Arrays.asList(token));
						System.out.println(tempList.get(0));
						System.out.println(tempList.get(1));
						System.out.println(tempList.get(2));
						System.out.println(tempList.get(3));
						System.out.println(tempList.get(4));
						System.out.println(tempList.get(5));
						System.out.println(tempList.get(6));
						System.out.println(tempList.get(7));
						
						pstmt.setString(1, fileNmAry[1].replace(".xlsx", ""));
	                    pstmt.setString(2, tempList.get(0));
	                    pstmt.setString(3, tempList.get(1));
	                    pstmt.setString(4, tempList.get(2));
	                    pstmt.setString(5, tempList.get(3));
	                    pstmt.setString(6, tempList.get(4));
	                    pstmt.setString(7, tempList.get(5));
	                    pstmt.setString(8, tempList.get(6));
	                    pstmt.setString(9, tempList.get(7));
	                    
	                    pstmt.addBatch();
	                    totCnt++;
						 
					}
					k++;
					
				}
				k=0;
				
				
				/*XSSFWorkbook workbook = new XSSFWorkbook(file);
				int rowindex=0;
	            int columnindex=0;
	            //시트 수 (첫번째에만 존재하므로 0을 준다)
	            //만약 각 시트를 읽기위해서는 FOR문을 한번더 돌려준다
	            XSSFSheet sheet=workbook.getSheetAt(0);
	            //행의 수
	            int rows=sheet.getPhysicalNumberOfRows();
	            for(rowindex=1;rowindex<rows;rowindex++){
	                //행을읽는다
	                XSSFRow row=sheet.getRow(rowindex);
	                if(row !=null){
	                    //셀의 수
	                    int cells=row.getPhysicalNumberOfCells();
	                    String[]  value= new String[8];
	                    for(columnindex=0; columnindex<=cells; columnindex++){
	                        //셀값을 읽는다
	                        XSSFCell cell=row.getCell(columnindex); 
	                        //셀이 빈값일경우를 위한 널체크
	                        if(cell==null){
	                            continue;
	                        }else{
	                        	value[columnindex]=cell.toString();
	                        } 
	                    }
	                     
	                    
	                    pstmt.setString(1, fileNmAry[1].replace(".xlsx", ""));
	                    pstmt.setString(2, value[0]);
	                    pstmt.setString(3, value[1]);
	                    pstmt.setString(4, value[2]);
	                    pstmt.setString(5, value[3]);
	                    pstmt.setString(6, value[4]);
	                    pstmt.setString(7, value[5]);
	                    pstmt.setString(8, value[6]);
	                    pstmt.setString(9, value[7]);
	                    
	                    //System.out.println(pstmt);
	                    
	                    pstmt.addBatch();
	                    totCnt++;
	                     
	 
	                }
	            }*/
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		    pstmt.executeBatch();
		    
		}
		System.out.println("totCnt========> 총 : "+totCnt+"건 완료");
	}
	
}
