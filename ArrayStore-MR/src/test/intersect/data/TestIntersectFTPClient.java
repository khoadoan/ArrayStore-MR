package intersect.data;

import java.io.IOException;

import org.junit.Test;

public class TestIntersectFTPClient {
  @Test
  public void testGetCloudsat() throws IOException {
    IntersectFTPClient ftp = new IntersectFTPClient();
    
    ftp.retrieveCloudsat(IntersectFTPClient.CLOUDSAT_2B_GEOPROF, IntersectFTPClient.CLOUDSAT_2B_GEOPROF_RELEASE, 2007, 1, "/tmp", Integer.MAX_VALUE);
  }
  
  @Test 
  public void testGetTRMM() throws IOException {
    IntersectFTPClient ftp = new IntersectFTPClient();
    ftp.retrieveTRMM(IntersectFTPClient.TRMM_1C21, 2007, 1, "/tmp", Integer.MAX_VALUE);
  }
}
