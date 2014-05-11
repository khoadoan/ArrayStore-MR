package array.io;


import io.type.matrix.DoubleMatrixAnyD;
import io.type.matrix.SparseDoubleMatrixAnyD;

import org.junit.Test;


public class TestSparseDoubleMatrixAnyD {
  @Test
  public void testViewSlice() {
    int[] dims = new int[] {2, 2, 3, 2};
    SparseDoubleMatrixAnyD m = new SparseDoubleMatrixAnyD(dims);
    
    int no = 0;
    for(int i=0; i < 2; i++) {
      for(int j=0; j < 2; j++) {
        for(int k=0; k < 3; k++) {
          for(int l=0; l < 2; l++) {
            m.set(no++, i, j, k, l);
          }
        }
      }
    }
    
    System.out.println("Intial Matrix");
    System.out.println(m.toString());
    
    DoubleMatrixAnyD mm = m.viewSlice(new int[] {0}, new int[] {0});
    System.out.println("\n\n New Matrix: Slice on i=0");
    System.out.println(mm.toString());
    
    mm = m.viewSlice(new int[] {0}, new int[] {1});
    System.out.println("\n\n New Matrix: Slice on i=1");
    System.out.println(mm.toString());
    
    mm = m.viewSlice(new int[] {0,2}, new int[] {1,1});
    System.out.println("\n\n New Matrix: Slice on i=1, k=1");
    System.out.println(mm.toString());
  }
}
