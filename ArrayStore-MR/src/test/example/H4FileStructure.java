/*****************************************************************************
 * Copyright by The HDF Group.                                               *
 * Copyright by the Board of Trustees of the University of Illinois.         *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of the HDF Java Products distribution.                  *
 * The full copyright notice, including terms governing use, modification,   *
 * and redistribution, is contained in the files COPYING and Copyright.html. *
 * COPYING can be found at the root of the source code distribution tree.    *
 * Or, see http://hdfgroup.org/products/hdf-java/doc/Copyright.html.         *
 * If you do not have access to either file, you may request a copy from     *
 * help@hdfgroup.org.                                                        *
 ****************************************************************************/

package example;

import java.util.Vector;

import ncsa.hdf.object.Dataset;
import ncsa.hdf.object.Datatype;
import ncsa.hdf.object.FileFormat;
import ncsa.hdf.object.Group;
import ncsa.hdf.object.HObject;
import ncsa.hdf.object.h4.H4File;
import array.utils.JArrayUtils;
// the common object package
// the HDF4 implementation

/**
 * <p>Title: HDF Object Package (Java) Example</p>
 * <p>Description: this example shows how to retrieve HDF file structure using the
 * "HDF Object Package (Java)". The example created the group structure
 * and datasets, and print out the file structure:
 * <pre>
 *     "/" (root)
 *         integer arrays
 *             2D 32-bit integer 20x10
 *             3D unsigned 8-bit integer 20x10x5
 *         float arrays
 *             2D 64-bit double 20x10
 *             3D 32-bit float  20x10x5
 * </pre>
 * </p>
 *
 * @author Peter X. Cao
 * @version 2.4
 */
public class H4FileStructure
{
    private static String fname = "C:/school/CTIntertersect/data/sample/CloudSat/2007001005141_03607_CS_2B-GEOPROF_GRANULE_P_R04_E02.hdf";
//    private static String fname = "C:\\school\\CTIntertersect\\data\\sample\\TRMM\\1C21.20070101.52013.7.HDF";
    private static long[] dims2D = {20, 10};
    private static long[] dims3D = {20, 10, 5};

    public static void main( String args[] ) throws Exception
    {
        if(args.length == 1)
          fname = args[0];
        // create the file and add groups ans dataset into the file
//        createFile();

        // retrieve an instance of H4File
        FileFormat fileFormat = FileFormat.getFileFormat(FileFormat.FILE_TYPE_HDF4);

        if (fileFormat == null)
        {
            System.err.println("Cannot find HDF4 FileFormat.");
            return;
        }

        // open the file with read-only access
        FileFormat testFile = fileFormat.open(fname, FileFormat.READ);

        if (testFile == null)
        {
            System.err.println("Failed to open file: "+fname);
            return;
        }

        // open the file and retrieve the file structure
        testFile.open();
        Group root = (Group)((javax.swing.tree.DefaultMutableTreeNode)testFile.getRootNode()).getUserObject();

        printGroup(root, "");

        // close file resource
        testFile.close();
    }

    /**
     * Recursively print a group and its members.
     * @throws Exception
     */
    private static void printGroup(Group g, String indent) throws Exception
    {
        if (g == null)
            return;

        java.util.List members = g.getMemberList();

        int n = members.size();
        indent += "    ";
        HObject obj = null;
        for (int i=0; i<n; i++)
        {
            obj = (HObject)members.get(i);
            if (obj instanceof Group)
            {
              System.out.println(indent+obj);
              printGroup((Group)obj, indent);
            } else if(obj instanceof Dataset) {
              Dataset ds = (Dataset)obj;
//              System.out.print("," + ds.getName());
              if (ds.getRank() <= 0) {
                ds.init();
              }
              System.out.println(indent+obj+"(" + ds.getClass() + ")");
              System.out.println(indent+indent+ "Full Name: " + ds.getFullName());
              Object dso = ds.read();
              if(ds.getDatatype() != null)
              {  
                System.out.println(indent+indent+ "Type: " + ds.getDatatype().getDatatypeDescription() + "-" + ds.getDatatype().getDatatypeClass() +  "(" + dso.getClass() + ")");
                if(dso instanceof Vector) {
                  Object dsov = ((Vector)dso).get(0);
                  System.out.println(indent+indent+ "Length: " + "(" + dsov.getClass() + ")" + JArrayUtils.length(dsov));
                } else {
                  System.out.println(indent+indent+ "Length: " + JArrayUtils.length(dso));
                }
              }
              
              System.out.println(indent+indent+ "Rank: " + ds.getRank());
              print(indent + indent + "Dims", ds.getDims(), " x ");
              print(indent + indent +  "Chunk Size", ds.getChunkSize(), " x ");
              
            }
        }
        System.out.println();
    }
    
    private static void print(String label, long[] a, String delimiter) {
      if(a != null) {
        System.out.print(label + ": " + a[0]);
        for(int d=1; d<a.length; d++) {
          System.out.print(delimiter + a[d]);
        }
        System.out.println();
      }
    }
    
    private static void print(String label, String[] a, String delimiter) {
      if(a != null) {
        System.out.println(label + ": " + a[0]);
        for(int d=1; d<a.length; d++) {
          System.out.print(delimiter + a[d]);
        }
      }
    }

    /**
     * create the file and add groups ans dataset into the file,
     * which is the same as javaExample.H4DatasetCreate
     * @see javaExample.H4DatasetCreate
     * @throws Exception
     */
    private static void createFile() throws Exception
    {
        // retrieve an instance of H4File
        FileFormat fileFormat = FileFormat.getFileFormat(FileFormat.FILE_TYPE_HDF4);

        if (fileFormat == null)
        {
            System.err.println("Cannot find HDF4 FileFormat.");
            return;
        }

        // create a new file with a given file name.
        H4File testFile = (H4File)fileFormat.create(fname);

        if (testFile == null)
        {
            System.err.println("Failed to create file:"+fname);
            return;
        }

        // open the file and retrieve the root group
        testFile.open();
        Group root = (Group)((javax.swing.tree.DefaultMutableTreeNode)testFile.getRootNode()).getUserObject();

        // create groups at the root
        Group g1 = testFile.createGroup("integer arrays", root);
        Group g2 = testFile.createGroup("float arrays", root);

        // create 2D 32-bit (4 bytes) integer dataset of 20 by 10
        Datatype dtype = testFile.createDatatype(
            Datatype.CLASS_INTEGER, 4, Datatype.NATIVE, Datatype.NATIVE);
        Dataset dataset = testFile.createScalarDS
            ("2D 32-bit integer 20x10", g1, dtype, dims2D, null, null, 0, null);

        // create 3D 8-bit (1 byte) unsigned integer dataset of 20 by 10 by 5
        dtype = testFile.createDatatype(
            Datatype.CLASS_INTEGER, 1, Datatype.NATIVE, Datatype.SIGN_NONE);
        dataset = testFile.createScalarDS
            ("3D 8-bit unsigned integer 20x10x5", g1, dtype, dims3D, null, null, 0, null);

        // create 2D 64-bit (8 bytes) double dataset of 20 by 10
        dtype = testFile.createDatatype(
            Datatype.CLASS_FLOAT, 8, Datatype.NATIVE, -1);
        dataset = testFile.createScalarDS
            ("2D 64-bit double 20x10", g2, dtype, dims2D, null, null, 0, null);

        // create 3D 32-bit (4 bytes) float dataset of 20 by 10 by 5
        dtype = testFile.createDatatype(
            Datatype.CLASS_FLOAT, 4, Datatype.NATIVE, -1);
        dataset = testFile.createScalarDS
            ("3D 32-bit float  20x10x5", g2, dtype, dims3D, null, null, 0, null);

        // close file resource
        testFile.close();
    }

}