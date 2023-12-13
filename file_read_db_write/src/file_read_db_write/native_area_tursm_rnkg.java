package file_read_db_write;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

public class native_area_tursm_rnkg {
	public static void main(String[] agrs) throws FileNotFoundException {
		//엑셀 파일 경로
		File dir = new File("E:\\01. 프로젝트\\03. J-TaaS\\97. 관광데이터랩자료\\03. 내비게이션 지역별 관광지 순위\\전체데이터");
		File files[] = dir.listFiles();
		
		//query 문
		String query_start = "INSERT INTO jtassbigdata.native_area_tursm_rnkg (STD_YR_MM, BSC_LCGV_NM, TRRSRT_NM, ROAD_NM_ADDR, MDFG, SMCL, SRH_CNT) VALUES(";
		String query_end = ");";
		
		//파일 명 
		String fileNm = "";

		for (int i = 0; i < files.length; i++) {
		    System.out.println("-- file: " + files[i]);
		    FileInputStream file = new FileInputStream(files[i]);
		    
		    //년월 및 기초 지자체 명 찾기
		    fileNm =  files[i].toString();
		    String[] fileNmAry = fileNm.split("_");
		    
		    try {
				XSSFWorkbook workbook = new XSSFWorkbook(file);
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
	                    String[]  value= new String[6];
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
	                    
	                    System.out.println(query_start+"'"+fileNmAry[4].replace(".xlsx", "")+"','"+fileNmAry[1]+"','"+value[1]+"','"+value[2]+"','"+value[3]+"','"+value[4]+"',"+value[5].replace(".0", "")+ query_end);
	 
	                }
	            }
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		 
		    
		}
	}
	
}
