package hadoop.utils;

import org.apache.hadoop.io.ByteWritable;

public interface Constants {

  public int CODE_CHUNK_DATA = 1;
  public int CODE_CHUNK_OVERLAP = 2;
    
  public static final String DIMENSION_STR = "dims";
  public static final String CHUNK_STR = "chunks";
  public static final String OVERLAP_STR = "overlaps";
  
  public static final int CODE_CS_DATA = 0;
  public static final int CODE_CS_OVERLAP = 1;
  public static final int CODE_TR_DATA = 2;
  public static final int CODE_TR_OVERLAP = 3;
  
  public static final String METHOD_OLD = "old";
}
