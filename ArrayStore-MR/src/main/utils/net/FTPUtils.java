package utils.net;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.InetAddress;

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;

public class FTPUtils {

  /**
   * @param args
   * @throws Exception 
   */
  public static void main(String[] args) throws Exception {
    String cmd = args[0];
    
    String host = args[1];
    int port = Integer.parseInt(args[2]);
    String username = args[3];
    String password = args[4];
    if(cmd.equals("DPUT")) {
      String srcDirName = args[5];
      String destDirName = args[6];
      
      put(host, port, username, password, srcDirName, destDirName);
    }

  }

  public static void put(String host, int port, String username, String password, String srcDirName, String destDirName) throws Exception {
    put(getConnection(host, port, username, password), srcDirName, destDirName);
  }
  
  public static void put(FTPClient client, String srcDirName, String destDirName) throws Exception {
    client.setControlKeepAliveTimeout(60);
    client.setFileTransferMode(FTP.COMPRESSED_TRANSFER_MODE);
    client.setFileType(FTP.BINARY_FILE_TYPE);
    
    File srcDir = new File(srcDirName);
    
    System.out.println("PUT DIRECTORY: ");
    for(File file: srcDir.listFiles()) {
      System.out.println("\t Putting " + file.getName());
      putFile(client, file.getAbsolutePath(), file.getName(), destDirName);
    }
  }
  
  public static void putFile(FTPClient ftp, String localFileFullName, String fileName, String hostDir)
      throws Exception {
    InputStream input = null;
  try{
    input = new FileInputStream(new File(localFileFullName));
    ftp.storeFile(hostDir + "/" + fileName, input);
  } catch (Exception ex) {
    throw new RuntimeException("Error uploading " + localFileFullName);
  } finally {
    if(input != null)
      input.close();
  }
}
  
  public static void get() {
    
  }
  
  public static FTPClient getConnection(String host, int port, String username, String password) { 
    FTPClient ftp = new FTPClient();
    try {
      ftp.connect(InetAddress.getByName(host), port);
      ftp.enterLocalPassiveMode();
      int reply = ftp.getReplyCode();
      System.out.println("Connected.");
      if (!FTPReply.isPositiveCompletion(reply))
      {
          ftp.disconnect();
          throw new RuntimeException("FTP server refused connection.");
      }

      if(username != null)
        ftp.login(username, password);
      else 
        ftp.login("anonymous", "anonymous");
      
      System.out.println("Logged in.");
    } catch (Exception e) {
      throw new RuntimeException("Exception while connecting to " + host  + " at " + port);
    } 
    return ftp;
  }
}
