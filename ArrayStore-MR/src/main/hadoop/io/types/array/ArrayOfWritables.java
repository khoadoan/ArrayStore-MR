package hadoop.io.types.array;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Array;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class ArrayOfWritables<T extends Writable> implements Writable {
  private static int instanceCount = 0;
  private T[] data = null;
  
  public static int getInstanceCount() {
    return instanceCount;
  }
  
  public ArrayOfWritables() {
    instanceCount++;
  }
  
  public ArrayOfWritables(T[] data) {
    instanceCount++;
    set(data);
  }
  
  public T get(int index) {
    return data[index];
  }
  
  public void set(T[] data){
    this.data = data;
  }
  
  public int length() {
    return this.data.length;
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    for(int i=0; i<data.length; i++) { 
      if(data[i] != null) {
        out.writeUTF(this.data[i].getClass().getCanonicalName());
        break;
      }
    }
        
    WritableUtils.writeVInt(out, data.length);
    for(int i=0; i<data.length; i++){
      if(data[i] == null) {
        //False for null, True for Non-null
        out.writeBoolean(false);
      } else {
        out.writeBoolean(true);
        data[i].write(out);
      }
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    String keyClassName = in.readUTF(); 
    Class clazz = null;
    int size = WritableUtils.readVInt(in);
    
    try {
      clazz = Class.forName(keyClassName);
      this.data = (T[])Array.newInstance(clazz, size);
    } catch(Exception ex) {
      throw new IOException("Cannot instantiate the array of " + keyClassName + " with size " + size);
    }
    
    for(int i=0; i<size; i++) {
      //Only read data if non null
      if(in.readBoolean()) {
        try {
          this.data[i] = (T) clazz.newInstance();
        } catch (Exception ex) {
          throw new IOException("Cannot instantiate the element at " + i + " of class " + keyClassName + " with exception " + ex.getMessage(), ex);
        }
        this.data[i].readFields(in);
      } else {
        this.data[i] = null;
      }
    }
  }
}
