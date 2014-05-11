import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;

import net.sf.sevenzipjbinding.ExtractOperationResult;
import net.sf.sevenzipjbinding.ISequentialOutStream;
import net.sf.sevenzipjbinding.ISevenZipInArchive;
import net.sf.sevenzipjbinding.SevenZip;
import net.sf.sevenzipjbinding.SevenZipException;
import net.sf.sevenzipjbinding.impl.RandomAccessFileInStream;
import net.sf.sevenzipjbinding.simple.ISimpleInArchive;
import net.sf.sevenzipjbinding.simple.ISimpleInArchiveItem;

public class ExtractItemsSimple {
    public static class SimpleFileWriter implements ISequentialOutStream {
      BufferedOutputStream writer = null;
//      int bytesRead = 0;
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
//          this.bytesRead += data.length;
          System.out.println("Bytes read: " + data.length);
        } catch (Exception e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
        return data.length;
      }
      
      public void close() throws IOException {
        if(this.writer != null) {
          this.writer.close();
        }
      }
    }
    public static void main(String[] args) {
        if (args.length == 0) {
            System.out.println("Usage: java ExtractItemsSimple <archive-name>");
            return;
        }
        RandomAccessFile randomAccessFile = null;
        ISevenZipInArchive inArchive = null;
        try {
            randomAccessFile = new RandomAccessFile(args[0], "r");
            inArchive = SevenZip.openInArchive(null, // autodetect archive type
                    new RandomAccessFileInStream(randomAccessFile));

            // Getting simple interface of the archive inArchive
            ISimpleInArchive simpleInArchive = inArchive.getSimpleInterface();

            System.out.println("   Hash   |    Size    | Filename");
            System.out.println("----------+------------+---------");

            for (ISimpleInArchiveItem item : simpleInArchive.getArchiveItems()) {
                final int[] hash = new int[] { 0 };
               
                if (!item.isFolder()) {
                    ExtractOperationResult result;

                    final long[] sizeArray = new long[1];
                    SimpleFileWriter writer = new SimpleFileWriter(item.getPath());
                    result = item.extractSlow(writer);
                    writer.close();
//                    result = item.extractSlow(new ISequentialOutStream() {
//                      public int write(byte[] data) throws SevenZipException {
//                          hash[0] ^= Arrays.hashCode(data); // Consume data
//                          sizeArray[0] += data.length;
//                          return data.length; // Return amount of consumed data
//                      }
//                  });

                    if (result == ExtractOperationResult.OK) {
                        System.out.println(String.format("%9X | %10s | %s", // 
                                hash[0], sizeArray[0], item.getPath()));
                    } else {
                        System.err.println("Error extracting item: " + result);
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("Error occurs: " + e);
            System.exit(1);
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
    }
}