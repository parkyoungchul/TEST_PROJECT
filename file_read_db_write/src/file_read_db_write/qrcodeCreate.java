package file_read_db_write;

import java.awt.Color;
import java.awt.Font;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Hashtable;

import javax.imageio.ImageIO;

import com.google.zxing.BarcodeFormat;
import com.google.zxing.EncodeHintType;
import com.google.zxing.WriterException;
import com.google.zxing.common.BitMatrix;
import com.google.zxing.qrcode.QRCodeWriter;
import com.google.zxing.qrcode.decoder.ErrorCorrectionLevel;

public class qrcodeCreate {
	private static java.sql.Statement stmt;

	public static void main(String[] args) throws WriterException, IOException, SQLException {
		//db 연결
		Connection conn = null;
		stmt = null;
		PreparedStatement pstmt = null; 
		ResultSet rs = null;
		
		File file = null;
		
		// gr
			
		try{ 
			file = new File("D:\\qrtest\\");
	        
	        if(!file.exists()) {
	            file.mkdirs();
	        }
	        
	        conn = DriverManager.getConnection("jdbc:mariadb://221.146.220.3:3308/challengetourcms", "challengetouruser", "challengetour!@#$pass");
        	String sql = "select POI_CD , TRRSRT_TNM , ADDR, LCTN  from challengetourcms.tb_poi_tursm_info_cll order by POI_CD desc limit 0,2000";  
	    
        	pstmt = conn.prepareStatement(sql);  
        	rs = pstmt.executeQuery(); 
        	
        	while(rs.next()){ 
        		 // 파일위치 및 파일명, 링크주소, 사이즈, 이미지타입
        		 file = new File("D:\\qrtest\\"+rs.getString("POI_CD")+"_"+rs.getString("LCTN")+"_"+rs.getString("TRRSRT_TNM")+".png");
        		 createQRImage(file, rs.getString("POI_CD"), 500, "png", rs.getString("TRRSRT_TNM"));
        		 System.out.println("QR Code 이미지를 확인 해보세요.");
        	 }
		}catch ( Exception e){
			System.out.println("QR Code OutputStream 도중 Excpetion 발생, {}"+e.getMessage());
				
		}finally{
	
	        if(rs != null){ try{ rs.close(); }catch(SQLException e){} }
	
	        if(pstmt != null){ try{ pstmt.close(); }catch(SQLException e){} }
	
	        if(conn != null){ try{ conn.close(); }catch(SQLException e){} }
	
	    }
	}

	private static void createQRImage(File qrFile, String qrCodeText, int size, String fileType, String trrsrtTnm) throws WriterException, IOException {
		Hashtable<EncodeHintType, ErrorCorrectionLevel> hintMap = new Hashtable<>();
		hintMap.put(EncodeHintType.ERROR_CORRECTION, ErrorCorrectionLevel.L);
		QRCodeWriter qrCodeWriter = new QRCodeWriter();
		BitMatrix byteMatrix = qrCodeWriter.encode(qrCodeText, BarcodeFormat.QR_CODE, size, size, hintMap);
		int matrixWidth = byteMatrix.getWidth();
		BufferedImage image = new BufferedImage(matrixWidth, matrixWidth, BufferedImage.TYPE_INT_RGB);
		image.createGraphics();
		Graphics2D graphics = (Graphics2D) image.getGraphics();
		// 배경색
		graphics.setColor(Color.WHITE);
		graphics.fillRect(0, 0, matrixWidth, matrixWidth);
		
		// QR코드색
		graphics.setColor(Color.BLACK);
		for (int i = 0; i < matrixWidth; i++) {
			for (int j = 0; j < matrixWidth; j++) {
				if (byteMatrix.get(i, j)) {
					graphics.fillRect(i, j, 1, 1);
				}
			}
		}
		graphics.setColor(Color.BLUE);
		graphics.setFont(new Font("TimesRoman", Font.BOLD, 20));
		graphics.drawString(trrsrtTnm+"("+qrCodeText+")", 50, 50); // 문자열 삽입
		ImageIO.write(image, fileType, qrFile);
	}

}

 