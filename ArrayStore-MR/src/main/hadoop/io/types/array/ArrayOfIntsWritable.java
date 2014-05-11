package hadoop.io.types.array;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

/**
 * An array of integers (VInt Format) that implements Writable class.
 * 
 * @author Khoa Doan
 */
public class ArrayOfIntsWritable implements WritableComparable<ArrayOfIntsWritable> {
  int[] array;

  /**
   * Constructor with no arguments.
   */
  public ArrayOfIntsWritable() {
    super();
  }

  /**
   * Constructor take in a one-dimensional array.
   * 
   * @param array
   *            input integer array
   */
  public ArrayOfIntsWritable(int[] array) {
    this.array = array;
  }

  /**
   * Constructor that takes the size of the array as an argument.
   * 
   * @param size
   *            number of integers in array
   */
  public ArrayOfIntsWritable(int size) {
    super();
    array = new int[size];
  }

  public void readFields(DataInput in) throws IOException {
    int size = in.readInt();
    array = new int[size];
    for (int i = 0; i < size; i++) {
      set(i, WritableUtils.readVInt(in));
    }
  }

  public void write(DataOutput out) throws IOException {
    out.writeInt(size());
    for (int i = 0; i < size(); i++) {
      WritableUtils.writeVInt(out, get(i));
    }
  }

  /**
   * Get a deep copy of the array.
   * 
   * @return a clone of the array
   */
  public int[] getClone() {
    return array.clone();
  }

  /**
   * Get a shallow copy of the array.
   * 
   * @return a pointer to the array
   */
  public int[] getArray() {
    return array;
  }

  /**
   * Set the array.
   * 
   * @param array
   */
  public void setArray(int[] array) {
    this.array = array;
  }

  /**
   * Returns the integer value at position <i>i</i>.
   * 
   * @param i
   *            index of integer to be returned
   * @return integer value at position <i>i</i>
   */
  public int get(int i) {
    return array[i];
  }

  /**
   * Sets the integer at position <i>i</i> to <i>f</i>.
   * 
   * @param i
   *            position in array
   * @param v
   *            integer value to be set
   */
  public void set(int i, int v) {
    array[i] = v;
  }

  /**
   * Returns the size of the integer array.
   * 
   * @return size of array
   */
  public int size() {
    return array.length;
  }

  public String toString() {
    String s = "[";
    for (int i = 0; i < size(); i++) {
      s += get(i) + ",";
    }
    s += "]";
    return s;
  }

  @Override
  public int compareTo(ArrayOfIntsWritable other) {
    int[] otherArray = other.getArray();
    //return (thisValue<thatValue ? -1 : (thisValue==thatValue ? 0 : 1));
    
    for(int i=0; i<array.length; i++) {
      if(array[i] < otherArray[i])
        return -1;
      else if(array[i] > otherArray[i])
        return 1;
    }
    
    return 0;
  }
  
  /**
   * Determine whether the distances between elements of 2 arrays are within some specified distances 
   * @param left
   * @param right
   * @param spec
   * @return
   */
  public static boolean within(ArrayOfIntsWritable left, ArrayOfIntsWritable right, int... spec) {
    return within(left.getArray(), right.getArray(), spec);
  }
  
  public static boolean within(long[] leftArray, long[] rightArray, int... spec) {    
    if(spec.length != leftArray.length || leftArray.length != rightArray.length)
      throw new IllegalArgumentException("Arrays and spec must have matching length");
    
    for(int i=0; i<leftArray.length; i++) {
      if(Math.abs(leftArray[i]-rightArray[i]) > spec[i]) {
        return false;
      }
    }
    
    return true;
  }
  
  public static boolean within(int[] leftArray, int[] rightArray, int... spec) {    
    if(spec.length != leftArray.length || leftArray.length != rightArray.length)
      throw new IllegalArgumentException("Arrays and spec must have matching length");
    
    for(int i=0; i<leftArray.length; i++) {
      if(Math.abs(leftArray[i]-rightArray[i]) > spec[i]) {
        return false;
      }
    }
    
    return true;
  }
  
  public static double distance(ArrayOfIntsWritable left, ArrayOfIntsWritable right) {
    double distance = 0;
    int[] leftArray = left.getArray();
    int[] rightArray = right.getArray();
    
    if(leftArray.length != rightArray.length)
      throw new IllegalArgumentException("Arrays must have matching length");
    
    for(int i=0; i<leftArray.length; i++) {
      distance += (leftArray[i] - rightArray[i]) ^ 2;
    }
    
    return Math.sqrt(distance); 
  }
}
