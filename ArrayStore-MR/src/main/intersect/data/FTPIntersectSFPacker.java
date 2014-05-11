package intersect.data;

import intersect.hdf.CloudSatHDF4;
import intersect.hdf.IntersectHDF4;
import intersect.hdf.TRMMHDF4;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;

import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.log4j.Logger;

import data.utils.ZipFileExtracter;

public class FTPIntersectSFPacker extends IntersectSFPacker {
  private static Logger LOG = Logger.getLogger(FTPIntersectSFPacker.class);
  private static IntersectHDF4 value = null;
  private static int ERROR_TOL = 10;

  public static void main(String... args) throws Exception {
   String tempDirName = args[0];
   String readerClass = args[1];
   String sequenceFileName = args[2];
   int year = Integer.parseInt(args[3]);
   int startHDFDoy = Integer.parseInt(args[4]);
   int startFTPDoy = Integer.parseInt(args[5]);
   int endDoy = Integer.parseInt(args[6]);
   boolean downloadOnly = Boolean.parseBoolean(args[7]);
   
   IntersectFTPClient ftp = new IntersectFTPClient();

   String localHadoopFS = "file:///";
   SequenceFile.Writer writer = createSequenceFile(sequenceFileName, readerClass, localHadoopFS);
   File tempDir = new File(tempDirName);
   
   
   int errorCount = 0;
   for(int doy=startHDFDoy; doy<=endDoy; ) {
     String[] filenames = null;
     if(doy >= startFTPDoy) {
       if(readerClass.equals(CloudSatHDF4.class.getCanonicalName())) {
         filenames = ftp.retrieveCloudsat(IntersectFTPClient.CLOUDSAT_2B_GEOPROF, IntersectFTPClient.CLOUDSAT_2B_GEOPROF_RELEASE, year, doy, tempDirName, Integer.MAX_VALUE);
       } else if(readerClass.equals(TRMMHDF4.class.getCanonicalName())) {
         try {
           filenames = ftp.retrieveTRMM(IntersectFTPClient.TRMM_1C21, year, doy, tempDirName, Integer.MAX_VALUE);
         } catch (Exception ex) {
           System.out.println("ERROR DOWNLOADING " + year + "-" + doy);
           System.out.println(ex.toString());
         }
       }
       
       System.out.println("Downloaded " + filenames.length + " files.");
       //Regular Increment
       doy++;
     } else {
       File[] zFiles = tempDir.listFiles();
       filenames = new String[zFiles.length];
       for(int i=0; i<zFiles.length; i++) {
         filenames[i] = zFiles[i].getName();
       }
       System.out.println("Existing " + tempDir.listFiles().length + " files.");
       //Jump to next FTP download
       doy = startFTPDoy;
     }
     
     System.out.println("TOTAL: " + tempDir.listFiles().length + " files.");

     if(downloadOnly)
       continue;
    
     if(filenames == null || filenames.length == 0) {
       System.out.println("NO FILES: " + year + "-" + doy);
       continue;
     }
     
     for(String filename: filenames) {
       File zipFile = new File(tempDirName + "/" + filename);
       String[] extractedFilenames = null;
       //Unzip file to current directory
       try {
         extractedFilenames = ZipFileExtracter.extract(zipFile.getAbsolutePath(), ".");
         if(extractedFilenames != null) {
           for(String hdfFilename: extractedFilenames) {
             File hdfFile = new File(hdfFilename);
             if(hdfFilename.toLowerCase().endsWith("hdf")) {
               System.out.println("\t\t\t\t- Writing " + hdfFile.getName());
               writeToSequenceFile(readerClass, writer, hdfFile);
             }
             //Delete extracted file
             hdfFile.delete();
           }         
           //Delete Zip File
           //zipFile.delete();
         }
       } catch(Exception ex) {
         System.out.println("Skip " + zipFile.getName() + " because of " + ex.getMessage());
         if(++errorCount > ERROR_TOL) {
           throw new RuntimeException("Exceed error limit");
         }
       }
     }
   }
   System.out.print("done...");
   IOUtils.closeStream(writer);
   System.out.println(" closed stream");
 }
}
