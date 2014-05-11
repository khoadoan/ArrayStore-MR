package hadoop.io.types.array;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import array.utils.JArrayUtils;

public class ChunkKeyWritable implements WritableComparable<ChunkKeyWritable>{
  protected int index;
  protected String[] dName = null;
  protected int[][] dRange = null;
  
  public ChunkKeyWritable() {
    
  }
  
  public ChunkKeyWritable(int index, String[] dName, int[][] dRange) {
    this.index = index;
    //Check legal declaration
    if(dName != null && dName.length != dRange.length) {
      throw new RuntimeException("Expect the number of ranges to match the number of dimensions");
    }
    
    if(dName != null)
      this.dName = JArrayUtils.copy(dName);
    this.dRange = JArrayUtils.copy(dRange);
  }
  
  public void setIndex(int ndex) {
    this.index = index;
  }
  
  public int getIndex() {
    return this.index;
  }
  
  public void set(String name, int start, int end) {
    set(ArrayUtils.indexOf(dName, name), start, end);
  }
  
  public void set(int index, int start, int end) {
    int[] range= dRange[index];
    range[0] = start;
    range[1] = end;
  }
  
  public int[] get(String name) {
    return get(ArrayUtils.indexOf(dName, name));
  }
  
  public int[] get(int nameIndex) {
    return dRange[nameIndex];
  }
  
  public int[][] getRanges() {
    return this.dRange;
  }
  
  public void setRanges(int[][] ranges) {
    this.dRange = ranges;
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeVInt(out, index);
    WritableUtils.writeVInt(out, dRange.length);
    out.writeBoolean(dName != null);

    for(int i=0; i<dRange.length; i++) {
      if(dName != null) {
        WritableUtils.writeCompressedString(out, dName[i]); 
      }
      int[] range = dRange[i];
      WritableUtils.writeVInt(out, range[0]);
      WritableUtils.writeVInt(out, range[1]);
     }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    index = WritableUtils.readVInt(in);
    int nDims = WritableUtils.readVInt(in);
    boolean hasName = in.readBoolean();
    
    dName = new String[nDims];
    dRange = new int[nDims][2];
    
    for(int i=0; i<nDims; i++) {
      if(hasName)
        dName[i] = WritableUtils.readCompressedString(in);
      dRange[i][0] = WritableUtils.readVInt(in);
      dRange[i][1] = WritableUtils.readVInt(in);
    }
  }

  @Override
  public int compareTo(ChunkKeyWritable o) {
    return this.index < o.index ? -1 : ((this.index == o.index) ? 0 : 1); 
  }
}
