package hadoop.io.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.collections15.list.FastArrayList;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import data.load.simple.InsertJoinArraysFromText;

import array.utils.JArrayUtils;

/**
 * @author Khoa
 * Simple reprentation of data points inside a chunk.
 * TODO: need to change if this is low/high memory footprint.
 */
public class SimpleChunkDataWritable implements Writable{
  private FastArrayList<long[]> coordinates;
  private FastArrayList<double[]> attributes;
  
  private int rank;
  private int numAttributes;
  
  private void initialize() {
    coordinates = new FastArrayList<long[]>(1);
    attributes = new FastArrayList<double[]>(1);
  }
  
  public SimpleChunkDataWritable() {
    initialize();
  }
  
  public SimpleChunkDataWritable(long[] coordinates, double[] attributes) {
    initialize();
    
    this.rank = coordinates.length;
    this.numAttributes = attributes.length;
    this.add(coordinates, attributes);
  }
  
  public SimpleChunkDataWritable(int rank, int numAttributes) {
    initialize();
    
    this.rank = rank;
    this.numAttributes = numAttributes;
  }
    
  public double[] getAttributes(int index) {
    if(index >= size())
      throw new IndexOutOfBoundsException(index + " >= " + size());
    return this.attributes.get(index);
  }
  
  public long[] getCoordinates(int index) {
    if(index >= size())
      throw new IndexOutOfBoundsException(index + " >= " + size());
    return this.coordinates.get(index);
  }
  
  public void set(int i, long[] coordinates, double[] attributes) {
//    if(coordinates.length != rank || attributes.length != numAttributes)
//      throw new RuntimeException("Incorrect rank or number of attributes in SET: (" 
//              + coordinates.length + ", " + attributes.length + "), expected (" + this.rank + ", " + this.numAttributes + ")");
    
    if(this.coordinates == null){
      this.coordinates = new FastArrayList<long[]>();
      this.attributes = new FastArrayList<double[]>();
    }
    
    if(i > size()) {
      throw new java.lang.IndexOutOfBoundsException("Too far away. Must be <= size "); 
    } else if(i == size()) {
      this.coordinates.add(coordinates);
      this.attributes.add(attributes);
    } else {
      this.coordinates.set(i, coordinates);
      this.attributes.set(i, attributes);
    }
  }
  
  public void add(long[] coordinates, double[] attributes) {
//    if(coordinates.length != rank || attributes.length != numAttributes)
//      throw new RuntimeException("Incorrect rank or number of attributes in ADD: (" 
//              + coordinates.length + ", " + attributes.length + "), expected (" + this.rank + ", " + this.numAttributes + ")");
    this.coordinates.add(coordinates);
    this.attributes.add(attributes);
  }
  
  public void addCopy(long[] coordinates, double[] attributes) {
    this.coordinates.add(JArrayUtils.copy(coordinates));
    this.attributes.add(JArrayUtils.copy(attributes));
  }
  
  public int size() {
    return this.coordinates.size();
  }
  
  public int rank() {
    return this.rank;
  }
  
  public int numberOfAttributes() {
    return this.numAttributes;
  }
  
  public void addAll(SimpleChunkDataWritable other) {
//    InsertJoinArraysFromText.LOG.info("THIS: " + this.size() + ", THAT: " + other.size());
    for(int i=0; i<other.size(); i++) {
      this.add(other.getCoordinates(i), other.getAttributes(i));
    }
  }
  
  public void addAllCopy(SimpleChunkDataWritable other) {
    for(int i=0; i<other.size(); i++) {
      this.addCopy(other.getCoordinates(i), other.getAttributes(i));
    }
  }
  
  public void addAll(Iterable<SimpleChunkDataWritable> others) {
    Iterator<SimpleChunkDataWritable> iter = others.iterator();
    while(iter.hasNext()) {
      this.addAll(iter.next());
    }
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeVInt(out, rank());
    WritableUtils.writeVInt(out, numberOfAttributes());
    WritableUtils.writeVInt(out, size());
    
    for(int i=0; i<size(); i++) {
      long[] coords = this.coordinates.get(i);
      double[] attrs = this.attributes.get(i);
      
      for(int j=0; j<rank; j++) {
        WritableUtils.writeVLong(out, coords[j]);
      }
      
      for(int j=0; j<numAttributes; j++) {
        out.writeDouble(attrs[j]);
      }
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    rank = WritableUtils.readVInt(in);
    numAttributes = WritableUtils.readVInt(in);
    int size = WritableUtils.readVInt(in);
    
    for(int i=0; i<size; i++) {
      try {
        long[] coords = new long[rank];
        double[] attrs = new double[numAttributes];
        
        for(int j=0; j<rank; j++) {
          coords[j] = WritableUtils.readVLong(in);
        }
        
        for(int j=0; j<numAttributes; j++) {
          attrs[j] = in.readDouble();
        }
        this.add(coords, attrs);
      } catch(Exception ex) {
        throw new IOException("Rank of " + rank + ", numAttributes of " + numAttributes + ", size of " + size + ", failed at i = " + i, ex);
      }
    }
  }
  
  public void clear() {
    if(size() > 0) {
      this.attributes.clear();
      this.coordinates.clear();
    }
  }
  
  public int elements() {
    int elements = 0;
    for(int i=0; i<this.coordinates.size(); i++) {
      elements += this.coordinates.get(i).length;
    }
    return elements;
  }
}
