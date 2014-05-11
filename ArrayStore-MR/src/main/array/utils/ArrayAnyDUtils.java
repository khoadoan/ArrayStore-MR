package array.utils;

import org.apache.commons.collections15.list.FastArrayList;


public class ArrayAnyDUtils {
  /**
   * Compute index of the chunk containing the coordinates, given the sizes of 
   * the array's dimensions. The array's dimensions are assumed to start from 0. 
   * @param coords
   * @param dimensionSizes
   * @param chunkSizes
   * @return
   */  
    public static long getChunkIndex(long[] coords, long[] dimensionSizes, long[] chunkSizes) {
      long index = 0;
      long chunkStrides = 1;
      for(int i=coords.length-1; i >= 0; i--) {
        index += (coords[i] / chunkSizes[i]) * chunkStrides;
        if(i != 0) {
          chunkStrides *= (dimensionSizes[i] / chunkSizes[i]);
        }
      }
      return index; 
    }
    
    /**
     * Compute index of the chunk containing the coordinates, given the sizes of 
     * the array's dimensions. The array's dimensions are assumed to start from 0. 
     * @param coords
     * @param dimensionSizes
     * @param chunkSizes
     * @return
     */  
      public static long getChunkIndex(int[] coords, int[] dimensionSizes, int[] chunkSizes) {
        long index = 0;
        long chunkStrides = 1;
        for(int i=coords.length-1; i >= 0; i--) {
          index += (coords[i] / chunkSizes[i]) * chunkStrides;
          if(i != 0) {
            chunkStrides *= (dimensionSizes[i] / chunkSizes[i]);
          }
        }
        return index; 
      }
  
  /**
   * Compute the index of a chunk containing the coordinates, given ranges of the array's dimensions
   * @param coords
   * @param dimensionRanges
   * @param chunkSizes
   * @return
   * @throws InterruptedException 
   */
  public static long getChunkIndex(long[] coords, long[][] dimensionRanges, long[] chunkSizes) throws InterruptedException {
    long index = 0;
    long chunkStrides = 1;

    try {
      for(int i=coords.length-1; i >= 0; i--) {
        index += ((coords[i] - dimensionRanges[i][0])/ chunkSizes[i]) * chunkStrides;
        if(i != 0) {
          chunkStrides *= ((dimensionRanges[i][1] - dimensionRanges[i][0] + 1) / chunkSizes[i]);
        }
      }
    } catch(Exception ex) {
      throw new InterruptedException("Invalid argument: [" + JArrayUtils.toString(coords, ",") +  "] - [" + JArrayUtils.toString(chunkSizes, ",") + "]");
    }
    return index; 
  }
  
  public static long getChunkIndex(int[] coords, int[][] dimensionRanges, int[] chunkSizes) throws InterruptedException {
    long index = 0;
    long chunkStrides = 1;

    try {
      for(int i=coords.length-1; i >= 0; i--) {
        index += ((coords[i] - dimensionRanges[i][0])/ chunkSizes[i]) * chunkStrides;
        if(i != 0) {
          chunkStrides *= ((dimensionRanges[i][1] - dimensionRanges[i][0] + 1) / chunkSizes[i]);
        }
      }
    } catch(Exception ex) {
      throw new InterruptedException("Invalid argument: [" + JArrayUtils.toString(coords, ",") +  "] - [" + JArrayUtils.toString(chunkSizes, ",") + "]");
    }
    return index; 
  }
    
  public static long[][] getChunkRange(long index, long[][] dimensionRanges, long[] chunkSizes) {
    long[][] range = new long[dimensionRanges.length][2];
    int chunkStrides = 1;
    for(int i=dimensionRanges.length-1; i>=0; i--) {
      long dimensionSize = dimensionRanges[i][1] - dimensionRanges[i][0] + 1;
      long chunkCoord = (long) ((index / chunkStrides) % (dimensionSize / chunkSizes[i]));
      range[i][0] = chunkCoord * chunkSizes[i] + dimensionRanges[i][0];
      range[i][1] = range[i][0] + chunkSizes[i] - 1;
      index -= chunkCoord * chunkStrides;
      if(i != 0) {
        chunkStrides *= (dimensionSize / chunkSizes[i]);
      }
    }
    
    return range;
  }
  
  /**
   * Rebase the dimensions of an array so that it starts from 0
   * @param orgDims
   * @return
   */
  public static long[] rebase(long[][] orgDims) {
    long[] newDims = new long[orgDims.length];
    
    for(int i=0; i<orgDims.length; i++){
      newDims[i] = orgDims[i][1] - orgDims[i][0] + 1;
    }
    
    return newDims;
  }
  
  /**
   * Rebase, but with string argument. Format: "s1-e1[,s2-e2]"
   * @param orgDimsStr
   * @return
   */
  public static long[] rebase(String orgDimsStr) {
    String[] a = orgDimsStr.split(",");
    long[] newDims = new long[a.length];
    for(int i=0; i<a.length; i++) {
      String[] e = a[i].split(":");
      newDims[i] = Integer.parseInt(e[1]) - Integer.parseInt(e[0]) + 1;
    }
    return newDims;
  }
  
  public static long[] getOverlapChunkIndices (long[] coords, long[][] dimRanges, long[] chunkSizes, long[] overlaps) throws InterruptedException {
    long currentChunkIndex = getChunkIndex(coords, dimRanges, chunkSizes);
    FastArrayList<Long> l = new FastArrayList<Long>();
    long strides = 1;
    for(int i=coords.length - 1; i>=0; i--) {
      long over = (coords[i]-dimRanges[i][0] + 1) % chunkSizes[i];
      long under = chunkSizes[i] - over + 1;
      if(coords[i]-dimRanges[i][0]+1 > chunkSizes[i] && over <= overlaps[i]) {
        l.add(currentChunkIndex - strides);
      } 
      if(dimRanges[i][1]-coords[i]+1 > chunkSizes[i] && under <= overlaps[i]) {
        l.add(currentChunkIndex + strides);
      }
      strides *= (dimRanges[i][1] - dimRanges[i][0] + 1) / chunkSizes[i];
    }

    return l.size() == 0 ? null:JArrayUtils.toPrimitiveTypes(l.toArray(new Long[l.size()]));
  }
  
  public static long[] getOverlapChunkIndices (int[] coords, int[][] dimRanges, int[] chunkSizes, int[] overlaps) throws InterruptedException {
    long currentChunkIndex = getChunkIndex(coords, dimRanges, chunkSizes);
    FastArrayList<Long> l = new FastArrayList<Long>();
    long strides = 1;
    for(int i=coords.length - 1; i>=0; i--) {
      long over = (coords[i]-dimRanges[i][0] + 1) % chunkSizes[i];
      long under = chunkSizes[i] - over + 1;
      if(coords[i]-dimRanges[i][0]+1 > chunkSizes[i] && over <= overlaps[i]) {
        l.add(currentChunkIndex - strides);
      } 
      if(dimRanges[i][1]-coords[i]+1 > chunkSizes[i] && under <= overlaps[i]) {
        l.add(currentChunkIndex + strides);
      }
      strides *= (dimRanges[i][1] - dimRanges[i][0] + 1) / chunkSizes[i];
    }

    return l.size() == 0 ? null:JArrayUtils.toPrimitiveTypes(l.toArray(new Long[l.size()]));
  }
} 


