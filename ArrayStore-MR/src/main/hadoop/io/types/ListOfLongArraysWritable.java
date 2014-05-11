package hadoop.io.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.collections15.list.FastArrayList;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;


public class ListOfLongArraysWritable implements Writable{
  private FastArrayList<long[]> data = null;
  private int rank;

  public ListOfLongArraysWritable(int rank) {
    this.rank = rank;
    data = new FastArrayList<long[]>();
  }

  public void readFields(DataInput in) throws IOException {
    int size = WritableUtils.readVInt(in);
    this.rank = WritableUtils.readVInt(in);
    data = new FastArrayList<long[]>(size);
    for(int i=0; i<size; i++)
    {
      long[] item = new long[this.rank];
      for(int j=0; j<this.rank; j++)
        item[j] = WritableUtils.readVLong(in);
      add(item);
    }
  }

  public void write(DataOutput out) throws IOException {
    WritableUtils.writeVInt(out, data.size());
    WritableUtils.writeVInt(out, this.rank);
    for(int i=0; i<data.size(); i++)
    {
      long[] item = data.get(i);
      for(int j=0; j<this.rank; j++)
        WritableUtils.writeVLong(out, item[j]);
    }
  }
  
  public void add(long[] item) {
    data.add(item);
  }
  
  public void addAll(ListOfLongArraysWritable other) {
    for(int i=0; i<other.size(); i++) {
      data.add(other.get(i));
    }
  }
  
  public long[] get(int i) {
    return data.get(i);
  }
  
  public int size() {
    return data.size();
  }
}

