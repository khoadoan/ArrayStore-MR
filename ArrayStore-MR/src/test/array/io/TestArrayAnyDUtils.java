package array.io;

import org.junit.Test;

import array.utils.ArrayAnyDUtils;

public class TestArrayAnyDUtils {
  @Test
  public void testGetChunkIndexFromCoordinates() throws InterruptedException {
    int[] coords = new int[] {-3878,-1799,-4723,228814,2007,111};
    int[][] dimSizes = new int[][] {new int[] {-6371,6372},
                                    new int[] {-6371,6372},
                                    new int[] {-6371,6372},
                                    new int[] {0,863999},
                                    new int[] {2006,2010},
                                    new int[] {1,366}};
    int[] chunks = new int[] {6372,6372,6372,60000,1,6};
    
    System.out.println(ArrayAnyDUtils.getChunkIndex(coords, dimSizes, chunks));
  }
  
//  @Test
//  public void testGetChunkIndexFromCoordinates() {
//    int[][] dimRanges = new int[][] { new int[] {-6371,6372},
//                                      new int[] {-6371,6372},
//                                      new int[] {-6371,6372},
//                                      new int[] {2000,2019},
//                                      new int[] {1,366},
//                                      new int[] {0, 863999} };
//    int[] dimSizes = new int[] { 4, 6 };
//    int[] chunkSizes = new int[] {6, 6, 6, 2, 2, 1000};
//
////    assertEquals(1, ArrayAnyDUtils.getChunkIndex(new int[] {0, 2}, dimSizes, chunkSizes));
//    assertEquals(1, ArrayAnyDUtils.getChunkIndex(new int[] {-3157, -142, -5532, 2007, 111, 38690}, dimRanges, chunkSizes));
//    
////    assertEquals(4, ArrayAnyDUtils.getChunkIndex(new int[] {2, 3}, dimSizes, chunkSizes));
////    assertEquals(4, ArrayAnyDUtils.getChunkIndex(new int[] {4, 4}, dimRanges, chunkSizes));
//  }
//  
//  @Test
//  public void testGetChunkRangeFromChunkIndex() {
//    int[][] dimRanges = new int[][] { new int[] {2,5},
//                                      new int[] {1,6} };
//    int[] chunkSizes = new int[] {2, 2};
//    
//    long chunkIndex = ArrayAnyDUtils.getChunkIndex(new int[] {2, 3}, dimRanges, chunkSizes);
//    int[][] chunkRanges = ArrayAnyDUtils.getChunkRange(chunkIndex, dimRanges, chunkSizes);
//    assertEquals(2, chunkRanges[0][0]);
//    assertEquals(3, chunkRanges[0][1]);
//    assertEquals(3, chunkRanges[1][0]);
//    assertEquals(4, chunkRanges[1][1]);
//    
//    chunkIndex = ArrayAnyDUtils.getChunkIndex(new int[] {4, 4}, dimRanges, chunkSizes);
//    chunkRanges = ArrayAnyDUtils.getChunkRange(chunkIndex, dimRanges, chunkSizes);
//    assertEquals(4, chunkRanges[0][0]);
//    assertEquals(5, chunkRanges[0][1]);
//    assertEquals(3, chunkRanges[1][0]);
//    assertEquals(4, chunkRanges[1][1]);
//    
//    dimRanges = new int[][] { new int[] {0,3},
//                              new int[] {0,3},
//                              new int[] {1,6} };
//    chunkSizes = new int[] {2, 2, 2};
//    chunkIndex = ArrayAnyDUtils.getChunkIndex(new int[] {3, 1, 5}, dimRanges, chunkSizes);
//    chunkRanges = ArrayAnyDUtils.getChunkRange(chunkIndex, dimRanges, chunkSizes);
//    assertEquals(2, chunkRanges[0][0]);
//    assertEquals(3, chunkRanges[0][1]);
//    assertEquals(0, chunkRanges[1][0]);
//    assertEquals(1, chunkRanges[1][1]);
//    assertEquals(5, chunkRanges[2][0]);
//    assertEquals(6, chunkRanges[2][1]);
//  }
//  
//  @Test
//  public void testGetOverlapIndicesFromCoordinates() {
//    int[][] dimRanges = new int[][] { new int[] {1, 24},
//                              new int[] {1, 24},
//                              new int[] {1, 24} };
//    int[] chunkSizes = new int[] {4, 4, 4};
//    int[] chunkOverlaps = new int[] {2, 2, 2};
//    
//    System.out.println("TEST 3-D");
//    assertEquals(null, ArrayAnyDUtils.getOverlapChunkIndices(new int[] {1, 1, 1}, dimRanges, chunkSizes, chunkOverlaps));
//    assertEquals(null, ArrayAnyDUtils.getOverlapChunkIndices(new int[] {23, 23, 23}, dimRanges, chunkSizes, chunkOverlaps));
//
//    System.out.println("Should be " + JArrayUtils.toString(new int[] {44, 49, 79}, ","));
//    System.out.println(JArrayUtils.toString(ArrayAnyDUtils.getOverlapChunkIndices(new int[] {7, 7, 7}, dimRanges, chunkSizes, chunkOverlaps), ","));
//    
//    dimRanges = new int[][] { new int[] {1, 24},
//        new int[] {1, 24}};
//    chunkSizes = new int[] {4, 4};
//    chunkOverlaps = new int[] {2, 2};
//    
//    System.out.println("Should be " + JArrayUtils.toString(new int[] {13, 8}, ","));
//    System.out.println(JArrayUtils.toString(ArrayAnyDUtils.getOverlapChunkIndices(new int[] {9, 10}, dimRanges, chunkSizes, chunkOverlaps), ","));
//
//    chunkOverlaps = new int[] {0, 0};
//    assertEquals(null, ArrayAnyDUtils.getOverlapChunkIndices(new int[] {9, 10}, dimRanges, chunkSizes, chunkOverlaps));
//    
//    chunkOverlaps = new int[] {3, 3};
//    System.out.println("Should be " + JArrayUtils.toString(new int[] {13, 15, 8}, ","));
//    System.out.println(JArrayUtils.toString(ArrayAnyDUtils.getOverlapChunkIndices(new int[] {9, 10}, dimRanges, chunkSizes, chunkOverlaps), ","));
//    
//    chunkOverlaps = new int[] {4, 4};
//    System.out.println("Should be " + JArrayUtils.toString(new int[] {13, 15, 8, 20}, ","));
//    System.out.println(JArrayUtils.toString(ArrayAnyDUtils.getOverlapChunkIndices(new int[] {9, 10}, dimRanges, chunkSizes, chunkOverlaps), ","));
//
//    chunkOverlaps = new int[] {2, 2};
//    System.out.println("Should be " + JArrayUtils.toString(new int[] {29}, ","));
//    System.out.println(JArrayUtils.toString(ArrayAnyDUtils.getOverlapChunkIndices(new int[] {22, 23}, dimRanges, chunkSizes, chunkOverlaps), ","));
//    
//    chunkOverlaps = new int[] {3, 3};
//    System.out.println("Should be " + JArrayUtils.toString(new int[] {29, 34}, ","));
//    System.out.println(JArrayUtils.toString(ArrayAnyDUtils.getOverlapChunkIndices(new int[] {22, 23}, dimRanges, chunkSizes, chunkOverlaps), ","));
//
//  }
}
