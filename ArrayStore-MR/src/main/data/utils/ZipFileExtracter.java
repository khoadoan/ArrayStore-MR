package data.utils;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;

import net.sf.sevenzipjbinding.ExtractOperationResult;
import net.sf.sevenzipjbinding.ISequentialOutStream;
import net.sf.sevenzipjbinding.ISevenZipInArchive;
import net.sf.sevenzipjbinding.SevenZip;
import net.sf.sevenzipjbinding.SevenZipException;
import net.sf.sevenzipjbinding.impl.RandomAccessFileInStream;
import net.sf.sevenzipjbinding.simple.ISimpleInArchive;
import net.sf.sevenzipjbinding.simple.ISimpleInArchiveItem;

public class ZipFileExtracter {
  public static class SimpleFileWriter implements ISequentialOutStream {
    BufferedOutputStream writer = null;
    long bytesRead = 0;
    public SimpleFileWriter(String filename) {
      try {
        writer = new BufferedOutputStream(new FileOutputStream(filename));
      } catch (FileNotFoundException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
    @Override
    public int write(byte[] data) throws SevenZipException {
      try {
        this.writer.write(data);
      } catch (Exception e) {
        throw new RuntimeException("Cannot write to file during extraction.", e);
      }
      bytesRead += data.length;
      return data.length;
    }
    
    public long close() throws IOException {
      if(this.writer != null) {
        this.writer.close();
      }
      
      return this.bytesRead;
    }
  }
  
  public static String[] extract(String compressedFilename, String savePath) {
    RandomAccessFile randomAccessFile = null;
    ISevenZipInArchive inArchive = null;
    String[] extractedFilenames = null;
    try {
        randomAccessFile = new RandomAccessFile(compressedFilename, "r");
        inArchive = SevenZip.openInArchive(null, // autodetect archive type
                new RandomAccessFileInStream(randomAccessFile));

        // Getting simple interface of the archive inArchive
        ISimpleInArchive simpleInArchive = inArchive.getSimpleInterface();
        extractedFilenames = new String[simpleInArchive.getNumberOfItems()];
        int count = 0;
        
        for (ISimpleInArchiveItem item : simpleInArchive.getArchiveItems()) {
            final int[] hash = new int[] { 0 };
            if (!item.isFolder()) {
                ExtractOperationResult result;

                long bytesRead = 0;
                if(simpleInArchive.getNumberOfItems() == 1) {
                  String compressedFilenameShort = new File(compressedFilename).getName();
                  extractedFilenames[count] = savePath + "/" + compressedFilenameShort.substring(0, compressedFilenameShort.lastIndexOf("."));
                } else {
                  extractedFilenames[count] = savePath + "/" + item.getPath();
                }
                SimpleFileWriter writer = new SimpleFileWriter(extractedFilenames[count]);
                result = item.extractSlow(writer);
                bytesRead = writer.close();
                
                if (result == ExtractOperationResult.OK) {
                    System.out.println(String.format("%10s | %s", // 
                            bytesRead, item.getPath()));
                } else {
                    System.err.println("Error extracting item: " + result);
                }
                count++;
            }
        }
    } catch (Exception e) {
        throw new RuntimeException("Error occurs: " + e);
    } finally {
        if (inArchive != null) {
            try {
                inArchive.close();
            } catch (SevenZipException e) {
                System.err.println("Error closing archive: " + e);
            }
        }
        if (randomAccessFile != null) {
            try {
                randomAccessFile.close();
            } catch (IOException e) {
                System.err.println("Error closing file: " + e);
            }
        }
    }
    
    return extractedFilenames;
  }
}
