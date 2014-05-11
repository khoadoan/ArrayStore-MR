package hadoop.io.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.collections15.list.FastArrayList;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;


public class ListOfDoubleArraysWritable implements Writable{
  private FastArrayList<double[]> data = null;
  private int rank;

  public ListOfDoubleArraysWritable(int rank) {
    this.rank = rank;
    data = new FastArrayList<double[]>();
  }

  public void readFields(DataInput in) throws IOException {
    int size = WritableUtils.readVInt(in);
    this.rank = WritableUtils.readVInt(in);
    data = new FastArrayList<double[]>(size);
    for(int i=0; i<size; i++)
    {
      double[] item = new double[this.rank];
      for(int j=0; j<this.rank; j++)
        item[j] = in.readDouble();
      add(item);
    }
  }

  public void write(DataOutput out) throws IOException {
    WritableUtils.writeVInt(out, data.size());
    WritableUtils.writeVInt(out, this.rank);
    for(int i=0; i<data.size(); i++)
    {
      double[] item = data.get(i);
      for(int j=0; j<this.rank; j++)
        out.writeDouble(item[j]);
    }
  }
  
  public void add(double[] item) {
    data.add(item);
  }
  
  public void addAll(ListOfDoubleArraysWritable other) {
    for(int i=0; i<other.size(); i++) {
      data.add(other.get(i));
    }
  }
  
  public double[] get(int i) {
    return data.get(i);
  }
  
  public int size() {
    return data.size();
  }
}

