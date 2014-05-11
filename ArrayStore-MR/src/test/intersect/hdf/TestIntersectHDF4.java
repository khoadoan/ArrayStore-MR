package intersect.hdf;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.Calendar;

import ncsa.hdf.object.FileFormat;

import org.junit.Test;

import array.utils.JArrayUtils;

public class TestIntersectHDF4 {
  public static void main(String... args) {
    try {
      CloudSatHDF4 cs = (CloudSatHDF4) IntersectHDF4.getReader(CloudSatHDF4.class.getCanonicalName());
      cs.open("/home/khoa/school/sample/CloudSat/001/cloudsat/hdf/2007001005141_03607_CS_2B-GEOPROF-LIDAR_GRANULE_P2_R04_E02.hdf");      
      for(int i=0; i<1000; i++) {
        cs.next();
        int[] dimensions = cs.getCoordinates();
        double[] attributes = cs.getAttributes();
        for(int dimension: dimensions) {
          System.out.print(dimension + "\t" );
        }
        
//        System.out.println();
//        for(double attribute: attributes) {
//          System.out.print("\t" + attribute + "\t");
//        }
        System.out.println();
      }
      cs.close();
    } catch(Exception e) {
      e.printStackTrace();
      fail();
    }
  }
  
  
//  @Test
//  public void testParseFilename() {
//    File file = new File("C:/school/CTIntertersect/data/sample/CloudSat/2007001005141_03607_CS_2B-GEOPROF_GRANULE_P_R04_E02.hdf");
//    Calendar t = CloudSatHDF4.parseFilename(file.getName());
//    
//    assertEquals(t.get(Calendar.YEAR), 2007);
//    assertEquals(t.get(Calendar.DAY_OF_YEAR), 1);
//    assertEquals(t.get(Calendar.DAY_OF_YEAR), 1);
//  }
  
//  @Test
//  public void testParseTRMMFilename() {
//    File file = new File("C:\\school\\CTIntertersect\\data\\sample\\TRMM\\1C21.20070101.52013.7.HDF");
//    Calendar t = TRMMHDF4.parseFilename(file.getName());
//    System.out.println(t.getTime());
//    assertEquals(2007, t.get(Calendar.YEAR));
//    assertEquals(1, t.get(Calendar.DAY_OF_YEAR));
//  }
//  
//  @Test 
//  public void testComputeActualTime() { 
//    File file = new File("C:\\school\\CTIntertersect\\data\\sample\\TRMM\\1C21.20070101.52013.7.HDF");
//    Calendar t = TRMMHDF4.parseFilename(file.getName());
//    
//    double[] currentRecord = new double[15];
//    
//    IntersectUtils.computeActualTime(currentRecord, t, 0);
//    assertEquals((int)currentRecord[2], 2007);
//    assertEquals((int)currentRecord[3], 1);
//    assertEquals((int)currentRecord[4], 0);
//    
//    
//    IntersectUtils.computeActualTime(currentRecord, t, 2);
//    assertEquals((int)currentRecord[2], 2007);
//    assertEquals((int)currentRecord[3], 1);
//    assertEquals((int)currentRecord[4], 2 * 1000);
//    
//    IntersectUtils.computeActualTime(currentRecord, t, IntersectUtils.MSECONDS_IN_DAY/1000-1);
//    assertEquals((int)currentRecord[2], 2007);
//    assertEquals((int)currentRecord[3], 1);
//    assertEquals((int)currentRecord[4], IntersectUtils.MSECONDS_IN_DAY-1000);
//  }
  
  @Test
  public void testReadSomeTRMMFile() throws Exception {
    try {
      TRMMHDF4 trmm = (TRMMHDF4) IntersectHDF4.getReader(TRMMHDF4.class.getCanonicalName());
      trmm.open("C:\\school\\CTIntertersect\\data\\sample\\TRMM\\1C21.20070102.52027.7.HDF");
      System.out.println(trmm.getLength());
      for(int i=0; i<10; i++) {
        trmm.next();
//        int[] dimensions = cs.getCoordinates();
//        double[] attributes = cs.getAttributes();
//        for(int dimension: dimensions) {
//          System.out.print(dimension + "\t" );
//        }
//        System.out.print("\n\t****");
//        for(double attribute: attributes) {
//          System.out.print(attribute+ "\t" );
//        }
        
        double[] record = trmm.getRecord();
        System.out.println(i + " " + JArrayUtils.toString(record, ", ", true));
//        System.out.println();
      }
      trmm.close();
    } catch(Exception e) {
      e.printStackTrace();
      fail();
    }
  }
  
  @Test
  public void testReadSomeFile() throws Exception {
    try {
      CloudSatHDF4 cs = (CloudSatHDF4) IntersectHDF4.getReader(CloudSatHDF4.class.getCanonicalName());
      cs.open("C:/school/CTIntertersect/data/sample/CloudSat/2007001005141_03607_CS_2B-GEOPROF_GRANULE_P_R04_E02.hdf");
      for(int i=0; i<1000; i++) {
        cs.next();
        int[] dimensions = cs.getCoordinates();
        double[] attributes = cs.getAttributes();
        for(int dimension: dimensions) {
          System.out.print(dimension + "\t" );
        }

        System.out.println();
      }
      cs.close();
    } catch(Exception e) {
      e.printStackTrace();
      fail();
    }
  }
  
  public void testReadVectors() throws Exception {
    FileFormat f = null;
    try {
      f = AbstractHDF4.openFile4Read("C:/school/CTIntertersect/data/sample/CloudSat/2007001005141_03607_CS_2B-GEOPROF_GRANULE_P_R04_E02.hdf");
      
      AbstractHDF4.readDoubleA(f, "2B-GEOPROF", "Geolocation Fields");

    } catch(Exception e) {
      System.out.println(e.getMessage());
      e.printStackTrace();
      fail();
    } finally {
      if(f != null)
        f.close();
    }
  }
}
