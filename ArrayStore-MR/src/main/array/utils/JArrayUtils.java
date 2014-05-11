package array.utils;

import intersect.algo.FindIntersections;

import javax.naming.OperationNotSupportedException;

import org.apache.log4j.Logger;

public class JArrayUtils {
  public static String[] subarray(String[] array, int[] indices) {
    String[] narray = new String[indices.length];
    for(int i=0; i<indices.length; i++) {
      narray[i] = array[indices[i]];
    }
    return narray;
  }
  
  public static int[][] add(int[][] a, int... e) {
    int n = a.length;
    int[][] na = new int[n+1][];
    for(int i=0; i<a.length; i++) {
      na[i] = new int[a[i].length];
      for(int j=0; j<a[i].length; j++) {
        na[i][j] = a[i][j];
      }
    }
    
    na[n] = new int[e.length];
    for(int i=0; i<e.length; i++) {
      na[n][i] = e[i];
    }
    return na;
  }
  
  public static String[] copy(String[] a){
    String[] na = new String[a.length];
    for(int i=0; i<a.length; i++) {
      na[i] = a[i];
    }
    return na;
  }
  
  public static int[] copy(int[] a) {
    int[] na = new int[a.length];
    for(int i=0; i<a.length; i++) {
      na[i] = a[i];
    }
    return na;
  }
  
  public static long[] copy(long[] a) {
    long[] na = new long[a.length];
    for(int i=0; i<a.length; i++) {
      na[i] = a[i];
    }
    return na;
  }
  
  public static double[] copy(double[] a) {
    double[] na = new double[a.length];
    for(int i=0; i<a.length; i++) {
      na[i] = a[i];
    }
    return na;
  }
  
  public static int[][] copy(int[][] a) {
    int[][] na = new int[a.length][];
    for(int i=0; i<a.length; i++) {
      na[i] = copy(a[i]);
    }
    return na;
  }
  
  public static long[][] copy(long[][] a) {
    long[][] na = new long[a.length][];
    for(int i=0; i<a.length; i++) {
      na[i] = copy(a[i]);
    }
    return na;
  }
  
  public static int[] arrayOf(int size, int n) {
    int[] a = new int[size];
    for(int i=0; i<size; i++) {
      a[i] = n;
    }
    return a;
  }
  
  public static int[] zeros(int size) {
    return new int[size];
  }
  
  public static int[] toInts(long[] a) {
    int[] na = new int[a.length];
    for(int i=0; i<a.length; i++) {
      na[i] = (int) a[i];
    }
    return na;
  }
  
  public static int[] toInts(String[] a) {
    return toInts(a, 0, a.length);
  }
  
  public static long[] toLongs(String[] a) {
    return toLongs(a, 0, a.length);
  }
  
  public static int[] toInts(String[] a, int from, int to) {
    int[] na = new int[to - from];
    int nai = 0;
    for(int ai=from; ai<to; ai++) {
      na[nai++] = Integer.parseInt(a[ai]);
    }
    return na;
  }
  
  public static long[] toLongs(String[] a, int from, int to) {
    long[] na = new long[to - from];
    int nai = 0;
    for(int ai=from; ai<to; ai++) {
      na[nai++] = Long.parseLong(a[ai]);
    }
    return na;
  }
  
  public static double[] toDoubles(int[] a) {
    double[] na = new double[a.length];
    for(int i=0; i<a.length; i++) {
      na[i] = (double) a[i];
    }
    return na;
  }
  
  public static double[] toDoubles(long[] a) {
    double[] na = new double[a.length];
    for(int i=0; i<a.length; i++) {
      na[i] = (double) a[i];
    }
    return na;
  }
  
  public static double[] toDoubles(String[] a) {
    return toDoubles(a, 0, a.length);
  }
  
  public static double[] toDoubles(String[] a, int from, int to) {
    double[] na = new double[to - from];
    int nai = 0;
    for(int ai=from; ai<to; ai++) {
      na[nai++] = Double.parseDouble(a[ai]);
    }
    return na;
  }
  
  public static double[] convertToDoubles(float[] a) {
    double[] na = new double[a.length];
    for(int i=0; i<a.length; i++) {
      na[i] = (double) a[i];
    }
    return na;
  }
  
  public static double[] convertToDoubles(short[] a) {
    double[] na = new double[a.length];
    for(int i=0; i<a.length; i++) {
      na[i] = (double) a[i];
    }
    return na;
  }
  
  public static double[] convertToDoubles(byte[] a) {
    double[] na = new double[a.length];
    for(int i=0; i<a.length; i++) {
      na[i] = (double) a[i];
    }
    return na;
  }
  
  public static double[] convertToDoubles(int[] a) {
    double[] na = new double[a.length];
    for(int i=0; i<a.length; i++) {
      na[i] = (double) a[i];
    }
    return na;
  }
  
  public static double[] convertToDoubles(long[] a) {
    double[] na = new double[a.length];
    for(int i=0; i<a.length; i++) {
      na[i] = (double) a[i];
    }
    return na;
  }
    
  public static double[] read2DFrom1DEncoding(double[] a, int col, int rows, int cols) {
    double[] na = new double[rows];
    for(int i=0; i<rows; i++) {
      na[i] = a[i*cols + col];
    }
    return na;
  }
  
  public static double[] read2DFrom1DEncoding(float[] a, int col, int rows, int cols) {
    double[] na = new double[rows];
    for(int i=0; i<rows; i++) {
      na[i] = a[i*cols + col];
    }
    return na;
  }
  
  public static double[] read2DFrom1DEncoding(int[] a, int col, int rows, int cols) {
    double[] na = new double[rows];
    for(int i=0; i<rows; i++) {
      na[i] = a[i*cols + col];
    }
    return na;
  }
  
  public static double[] read2DFrom1DEncoding(short[] a, int col, int rows, int cols) {
    double[] na = new double[rows];
    for(int i=0; i<rows; i++) {
      na[i] = a[i*cols + col];
    }
    return na;
  }
  
  public static double[] read2DFrom1DEncoding(byte[] a, int col, int rows, int cols) {
    double[] na = new double[rows];
    for(int i=0; i<rows; i++) {
      na[i] = a[i*cols + col];
    }
    return na;
  }
  
  public static double[] read2DFrom1DObjectEncoding(Object o, int col, int rows, int cols) throws OperationNotSupportedException {
    if(o instanceof int[]) {
      return read2DFrom1DEncoding((int[])o, col, rows, cols);
    } else if(o instanceof byte[]) {
      return read2DFrom1DEncoding((byte[])o, col, rows, cols);
    } else if(o instanceof short[]) {
      return read2DFrom1DEncoding((short[])o, col, rows, cols);
    } else if(o instanceof float[]) {
      return read2DFrom1DEncoding((float[])o, col, rows, cols);
    } else if(o instanceof double[]) {
      return read2DFrom1DEncoding((double[])o, col, rows, cols);
    } else {
      throw new OperationNotSupportedException("unsupported data type: " + o.getClass());
    }
  }
  
  public static double readOne2DFrom1DEncoding(double[] a, int col, int row, int cols) {
    return a[cols*row + col];
  }
  
  public static int[] parseInts(String s, String delimiter) {
    String[] sa = s.split(delimiter);
    int[] a = new int[sa.length];
    for(int i=0; i<sa.length; i++) {
      a[i] = Integer.valueOf(sa[i]);
    }
    return a;
  }
  
  public static long[] parseLongs(String s, String delimiter) {
    String[] sa = s.split(delimiter);
    long[] a = new long[sa.length];
    for(int i=0; i<sa.length; i++) {
      a[i] = Integer.valueOf(sa[i]);
    }
    return a;
  }
  
  public static int[][] parseIntArrays(String s, String aDelimiter, String eDelimiter) {
    String[] aStrings = s.split(aDelimiter);
    int[][] a = new int[aStrings.length][];
    for(int i=0; i<aStrings.length; i++) {
      String[] eStrings = aStrings[i].split(eDelimiter);
      a[i] = new int[eStrings.length];
      for(int j = 0; j < eStrings.length; j++) {
        a[i][j] = Integer.parseInt(eStrings[j]);
      }
    }
    return a;
  }
  
  public static long[][] parseLongArrays(String s, String aDelimiter, String eDelimiter) {
    String[] aStrings = s.split(aDelimiter);
    long[][] a = new long[aStrings.length][];
    for(int i=0; i<aStrings.length; i++) {
      String[] eStrings = aStrings[i].split(eDelimiter);
      a[i] = new long[eStrings.length];
      for(int j = 0; j < eStrings.length; j++) {
        a[i][j] = Long.parseLong(eStrings[j]);
      }
    }
    return a;
  }
  
  public static String[] parseStrings(String s, String delimiter) {
    return s.split(delimiter);
  }

  public static void copyTo(int[] source, int[] dest) {
    for(int i=0; i<source.length; i++) {
      dest[i] = source[i];
    }
  }
  
  public static void copyTo(double[] source, double[] dest) {
    for(int i=0; i<source.length; i++) {
      dest[i] = source[i];
    }
  }
  
  public static void subtract(int[] from, int[] to) {
    for(int i=0; i<from.length; i++) {
      from[i] -= to[i];
    }
  }
  
  public static void mod(int[] from, int[] to) {
    for(int i=0; i<from.length; i++) {
      from[i] %= to[i];
    }
  }
  
  public static void mod(long[] from, long[] to) {
    for(int i=0; i<from.length; i++) {
      from[i] %= to[i];
    }
  }
  
  public static String toString(int[] a, String delimiter) {
    if(delimiter == null) {
      delimiter = ", ";
    }
    StringBuilder s = new StringBuilder("[").append(a[0]);
    for(int i=1; i<a.length; i++) {
      s.append(delimiter).append(a[i]);
    }
    s.append("]");
    return s.toString();
  }
  
  public static String toString(int[][] a, String aDlm, String eDlm) {
    if(aDlm == null) {
      aDlm = ", ";
    }
    if(eDlm == null) {
      eDlm = "-";
    }
    StringBuilder s = new StringBuilder("[");
    for(int i=0; i<a.length; i++) {
      if(i > 0) {
        s.append(aDlm);
      }
      for(int j=0; j <a[i].length; j++)
      {
        if(j == 0)
          s.append(a[i][j]);
        else 
          s.append(eDlm).append(a[i][j]);
      }
    }
    s.append("]");
    return s.toString();
  }
  
  public static String toString(long[] a, String delimiter) {
    if(delimiter == null) {
      delimiter = ", ";
    }
    StringBuilder s = new StringBuilder("[").append(a[0]);
    for(int i=1; i<a.length; i++) {
      s.append(delimiter).append(a[i]);
    }
    s.append("]");
    return s.toString();
  }
  
  public static String toString(double[] a, String delimiter, boolean isFormatted) {
    if(delimiter == null) {
      delimiter = ", ";
    }
    StringBuilder s = null;
    if(isFormatted)
      s = new StringBuilder("[").append(String.format("%.5f", a[0]).replaceAll("\\.?0+$", ""));
    else
      s = new StringBuilder(String.format("%.5f", a[0]).replaceAll("\\.?0+$", ""));
    
    for(int i=1; i<a.length; i++) {
      s.append(delimiter).append(String.format("%.5f", a[i]).replaceAll("\\.?0+$", ""));
    }
    if(isFormatted)
      s.append("]");
    return s.toString();
  }
  
  public static String toString(int[] a, String delimiter, boolean isFormatted) {
    if(delimiter == null) {
      delimiter = ",";
    }
    StringBuilder s = null;
    if(isFormatted)
      s = new StringBuilder("[").append(a[0]);
    else
      s = new StringBuilder(String.valueOf(a[0]));

    for(int i=1; i<a.length; i++) {
      s.append(delimiter).append(a[i]);
    }
    if(isFormatted)
      s.append("]");
    
    return s.toString();
  }
  
  public static double[] average(int[][] a) {
    double[] na = new double[a.length];
    for(int i=0; i < a.length; i++) {
      double sum = 0;
      for(int j=0; j<a[i].length; j++) {
        sum += a[i][j];
      }
      na[i] = sum/a[i].length;
    }
    return na;
  }
  
  public static double[] average(byte[][] a) {
    double[] na = new double[a.length];
    for(int i=0; i < a.length; i++) {
      double sum = 0;
      for(int j=0; j<a[i].length; j++) {
        sum += a[i][j];
      }
      na[i] = sum/a[i].length;
    }
    return na;
  }
  
  public static double[] average(short[][] a) {
    double[] na = new double[a.length];
    for(int i=0; i < a.length; i++) {
      double sum = 0;
      for(int j=0; j<a[i].length; j++) {
        sum += a[i][j];
      }
      na[i] = sum/a[i].length;
    }
    return na;
  }
  
  public static double[] average(float[][] a) {
    double[] na = new double[a.length];
    for(int i=0; i < a.length; i++) {
      double sum = 0;
      for(int j=0; j<a[i].length; j++) {
        sum += a[i][j];
      }
      na[i] = sum/a[i].length;
    }
    return na;
  }
  
  public static double[] average(double[][] a) {
    double[] na = new double[a.length];
    for(int i=0; i < a.length; i++) {
      double sum = 0;
      for(int j=0; j<a[i].length; j++) {
        sum += a[i][j];
      }
      na[i] = sum/a[i].length;
    }
    return na;
  }
  
  public static double[] average(byte[] a, int rows) {
    int cols = a.length/rows;
    double[] na = new double[rows];
    for(int i=0; i < rows; i++) {
      double sum = 0;
      int r = i*cols;
      for(int j=0; j<cols; j++) {
        sum += a[r+j];
      }
      na[i] = sum/cols;
    }
    return na;
  }
  
  public static double[] average(short[] a, int rows) {
    int cols = a.length/rows;
    double[] na = new double[rows];
    for(int i=0; i < rows; i++) {
      double sum = 0;
      int r = i*cols;
      for(int j=0; j<cols; j++) {
        sum += a[r+j];
      }
      na[i] = sum/cols;
    }
    return na;
  }
  
  public static double[] average(int[] a, int rows) {
    int cols = a.length/rows;
    double[] na = new double[rows];
    for(int i=0; i < rows; i++) {
      double sum = 0;
      int r = i*cols;
      for(int j=0; j<cols; j++) {
        sum += a[r+j];
      }
      na[i] = sum/cols;
    }
    return na;
  }
  
  public static double[] average(float[] a, int rows) {
    int cols = a.length/rows;
    double[] na = new double[rows];
    for(int i=0; i < rows; i++) {
      double sum = 0;
      int r = i*cols;
      for(int j=0; j<cols; j++) {
        sum += a[r+j];
      }
      na[i] = sum/cols;
    }
    return na;
  }
  
  public static double[] average(double[] a, int rows) {
    int cols = a.length/rows;
    double[] na = new double[rows];
    for(int i=0; i < rows; i++) {
      double sum = 0;
      int r = i*cols;
      for(int j=0; j<cols; j++) {
        sum += a[r+j];
      }
      na[i] = sum/cols;
    }
    return na;
  }
  
  public static double[] average2D(Object o) throws OperationNotSupportedException {
    if(o instanceof int[][]) {
      return average((int[][])o);
    } else if(o instanceof byte[][]) {
      return average((byte[][])o);
    } else if(o instanceof short[][]) {
      return average((short[][])o);
    } else if(o instanceof float[][]) {
      return average((float[][])o);
    } else if(o instanceof double[][]) {
      return average((double[][])o);
    } else {
      throw new OperationNotSupportedException("unsupported data type: " + o.getClass());
    }
  }
  
  public static double[] average2D(Object o, int rows) throws OperationNotSupportedException {
    if(o instanceof int[]) {
      return average((int[])o, rows);
    } else if(o instanceof byte[]) {
      return average((byte[])o, rows);
    } else if(o instanceof short[]) {
      return average((short[])o, rows);
    } else if(o instanceof float[]) {
      return average((float[])o, rows);
    } else if(o instanceof double[]) {
      return average((double[])o, rows);
    } else if(o instanceof int[][]) {
      return average((int[][])o);
    } else if(o instanceof byte[][]) {
      return average((byte[][])o);
    } else if(o instanceof short[][]) {
      return average((short[][])o);
    } else if(o instanceof float[][]) {
      return average((float[][])o);
    } else if(o instanceof double[][]) {
      return average((double[][])o);
    } else {
      throw new OperationNotSupportedException("unsupported data type: " + o.getClass());
    }
  }
  
  public static int length(Object o) {
    if(o instanceof byte[]) {
      return ((byte[])o).length;
    } else if(o instanceof short[]) {
      return ((short[])o).length;
    } else if(o instanceof int[]) {
      return ((int[])o).length;
    } else if(o instanceof float[]) {
      return ((float[])o).length;
    } else if(o instanceof double[]) {
      return ((double[])o).length;
    } else {
      return -1;
    }
  }
  
  public static double distance(int[] left, int[] right, int from, int to) {
    if(left == null || right == null || left.length != right.length || left.length == 0 || right.length == 0)
      throw new IllegalArgumentException("Illegal argument: left=" + left.length + ", right=" + right.length);
    double d = 0;
    for(int i=from; i<to; i++) {
      d += Math.pow(left[i]-right[i], 2);
    }
    return Math.sqrt(d);
  }
  
  public static double distance(long[] left, long[] right, int from, int to) {
    if(left == null || right == null || left.length != right.length || left.length == 0 || right.length == 0)
      throw new IllegalArgumentException("Illegal argument: left=" + left.length + ", right=" + right.length);
    double d = 0;
    for(int i=from; i<to; i++) {
      d += Math.pow(left[i]-right[i], 2);
    }
    return Math.sqrt(d);
  }

  public static long[] toPrimitiveTypes(Long[] a) {
    long[] ret = new long[a.length];
    for(int i=0; i<a.length; i++) {
      ret[i] = a[i].longValue();
    }
    return ret;
  }
  
  public static boolean within(double[] leftArray, double[] rightArray, double[] dist) {
    for(int i=0; i<leftArray.length; i++) {
      if(Math.abs(leftArray[i]-rightArray[i]) > dist[i]) {
        return false;
      }
    }
    return true;
  }
 }
