package intersect.data;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.util.Calendar;

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPFileFilter;
import org.apache.commons.net.ftp.FTPReply;

public class IntersectFTPClient {
  public static String TRMM_1C21 = "1C21";
  
  public static String CLOUDSAT_2B_GEOPROF_LIDAR = "2b-geoprof-lidar.p2";
  public static String CLOUDSAT_2B_GEOPROF_LIDAR_RELEASE = "r04";
  public static String CLOUDSAT_2B_GEOPROF = "2b-geoprof";
  public static String CLOUDSAT_2B_GEOPROF_RELEASE = "r04";
  public static String TRMM_HOST = "198.118.195.88";
  public static String CLOUDSAT_HOST = "129.82.109.195";
  public static String CLOUDSAT_USERNAME = "trmm";
  public static String CLOUDSAT_PASSWORD = "tm332vi"; 
  public static int FTP_DEFAULT_PORT = 21;
  
  private FTPClient csClient = null;
  private FTPClient trmmClient = null;
  
  public FTPClient getTrmmClient() throws IOException {
    if(trmmClient == null)
    {  
      trmmClient = getConnection(TRMM_HOST, FTP_DEFAULT_PORT, null, null);
    }
    return trmmClient;
  }

  public FTPClient getCsClient() throws IOException {
    if(csClient == null)
    {  
      csClient = getConnection(CLOUDSAT_HOST, FTP_DEFAULT_PORT, CLOUDSAT_USERNAME, CLOUDSAT_PASSWORD);
    }
    return csClient;
  }

  public String[] retrieveCloudsat(String prod, String release, int year, int doy, String localPath, int size) throws IOException {
    FTPClient ftp = getCsClient();
    
    ftp.setFileTransferMode(FTP.COMPRESSED_TRANSFER_MODE);
    ftp.setFileType(FTP.BINARY_FILE_TYPE);
    
    String remoteDirectory = String.format("/users/trmm/%s.%s/%4d/%03d",
                                prod, release, year, doy);
    FTPFile[] filesToDownload = ftp.listFiles(remoteDirectory, new FTPFileFilter() {
      @Override
      public boolean accept(FTPFile f) {
        return f.getName().toLowerCase().endsWith("zip");
      }
    });
    
    System.out.println("Downloading from " + remoteDirectory);
    ftp.changeWorkingDirectory(remoteDirectory);
    
    String[] downloadFilenames = new String[Math.min(filesToDownload.length, size)];
    int count = 0;
    for(FTPFile f: filesToDownload) {
      if(size-- > 0) {
        downloadFilenames[count++] = f.getName();
        OutputStream output = new FileOutputStream(localPath + "/" + f.getName());
        System.out.println("Downloading " + f.getName());
        ftp.retrieveFile(f.getName(), output);
        output.close();
      } else {
        break;
      }
    }
    
    return downloadFilenames;
  }
  
  public String[] retrieveTRMM(String prod, int year, int doy, String localPath, int size) throws IOException {
    FTPClient ftp = getTrmmClient();
    
    String remoteDirectory = String.format("/data/s4pa/TRMM_L1/TRMM_%s/%4d/%03d",
                                prod, year, doy);
    
    ftp.setFileTransferMode(FTP.COMPRESSED_TRANSFER_MODE);
    ftp.setFileType(FTP.BINARY_FILE_TYPE);
    
    FTPFile[] filesToDownload = ftp.listFiles(remoteDirectory, new FTPFileFilter() {
      @Override
      public boolean accept(FTPFile f) {
        return f.getName().toLowerCase().endsWith("hdf.z");
      }
    });
    
    System.out.println("Downloading from " + remoteDirectory);
    ftp.changeWorkingDirectory(remoteDirectory);
    String[] downloadFilenames = new String[Math.min(filesToDownload.length, size)];
    int count = 0;
    for(FTPFile f: filesToDownload) {
      if(size-- > 0) {
        downloadFilenames[count++] = f.getName();
        OutputStream output = new FileOutputStream(localPath + "/" + f.getName());
        System.out.println("Downloading " + f.getName());
        ftp.retrieveFile(f.getName(), output);
        output.close(); 
      } else {
        break;
      }
    }
    
    return downloadFilenames;
  }
  
  
  protected FTPClient getConnection(String host, int port, String username, String password) { 
    FTPClient ftp = new FTPClient();
    try {
      ftp.connect(InetAddress.getByName(host), port);
      ftp.enterLocalPassiveMode();
      int reply = ftp.getReplyCode();
      if (!FTPReply.isPositiveCompletion(reply))
      {
          ftp.disconnect();
          throw new RuntimeException("FTP server refused connection.");
      }

      if(username != null)
        ftp.login(username, password);
      else 
        ftp.login("anonymous", "anonymous");
    } catch (Exception e) {
      throw new RuntimeException("Exception while connecting to " + host  + " at " + port);
    } 
    return ftp;
  }
  
  protected int getDayOfYear(int year, int month, int day) {
    Calendar cal = Calendar.getInstance();
    cal.set(year, month, day);
    
    return cal.get(Calendar.DAY_OF_YEAR);
  }
  
  protected int getFirstDayOfMonth(int year, int month) {
    return getDayOfYear(year, month, 1);
  }
  
  protected int getLastDayOfMonth(int year, int month) {
    Calendar cal = Calendar.getInstance();
    cal.set(year, month, 1);
    
    return cal.get(Calendar.DAY_OF_YEAR) + cal.getActualMaximum(Calendar.DAY_OF_MONTH) - 1;
  }

  public void printFiles(FTPClient ftp, String pathname) throws IOException {
    for(FTPFile f: ftp.listFiles(pathname)) {
      System.out.println(f.getName());
    }
  }
  
  public void printNames(FTPClient ftp, String pathname) throws IOException {
    for(String name: ftp.listNames(pathname)) {
      System.out.println(name);
    }
  }
  
  public void unzip(String filename, String toPathname) {
    
  }
}
