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

import ncsa.hdf.object.Dataset;
import ncsa.hdf.object.Datatype;
import ncsa.hdf.object.FileFormat;
import ncsa.hdf.object.Group;
import ncsa.hdf.object.h4.H4File;
// the common object package
// the HDF4 implementation

/**
 * <p>Title: HDF Object Package (Java) Example</p>
 * <p>Description: this example shows how to read/write HDF datasets using the
 * "HDF Object Package (Java)". The example creates an integer dataset,
 * and read and write data values:
 * <pre>
 *     "/" (root)
 *             2D 32-bit integer 20x10
 * </pre>
 * </p>
 *
 * @author Peter X. Cao
 * @version 2.4
 */
public class H4DatasetRead
{
    private static String fname = "H4DatasetRead.hdf";
    private static long[] dims2D = {20, 10};

    public static void main( String args[] ) throws Exception
    {
        // create the file and add groups ans dataset into the file
        createFile();

        // retrieve an instance of H4File
        FileFormat fileFormat = FileFormat.getFileFormat(FileFormat.FILE_TYPE_HDF4);

        if (fileFormat == null)
        {
            System.err.println("Cannot find HDF4 FileFormat.");
            return;
        }

        // open the file with read and write access
        FileFormat testFile = fileFormat.open(fname, FileFormat.WRITE);

        if (testFile == null)
        {
            System.err.println("Failed to open file: "+fname);
            return;
        }

        // open the file and retrieve the file structure
        testFile.open();
        Group root = (Group)((javax.swing.tree.DefaultMutableTreeNode)testFile.getRootNode()).getUserObject();

        // retrieve athe dataset "2D 32-bit integer 20x10"
        Dataset dataset = (Dataset)root.getMemberList().get(0);
        dataset.hasAttribute();
        int[] dataRead = (int[])dataset.read();

        // print out the data values
        System.out.println("\n\nOriginal Data Values");
        for (int i=0; i<20; i++)
        {
            System.out.print("\n"+dataRead[i*10]);
            for (int j=1; j<10; j++)
            {
                System.out.print(", "+dataRead[i*10+j]);
            }
        }

        // change data value and write it to file.
        for (int i=0; i<20; i++)
        {
            for (int j=0; j<10; j++)
            {
                dataRead[i*10+j]++;
            }
        }
        dataset.write(dataRead);

        // clearn and reload the data value
        int[] dataModified = (int[])dataset.read();

        // print out the modified data values
        System.out.println("\n\nModified Data Values");
        for (int i=0; i<20; i++)
        {
            System.out.print("\n"+dataModified[i*10]);
            for (int j=1; j<10; j++)
            {
                System.out.print(", "+dataModified[i*10+j]);
            }
        }

        // close file resource
        testFile.close();
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

        // set the data values
        int[] dataIn = new int[20*10];
        for (int i=0; i<20; i++)
        {
            for (int j=0; j<10; j++)
            {
                dataIn[i*10+j] = 1000+i*100+j;
            }
        }

        // create 2D 32-bit (4 bytes) integer dataset of 20 by 10
        Datatype dtype = testFile.createDatatype(
            Datatype.CLASS_INTEGER, 4, Datatype.NATIVE, Datatype.NATIVE);
        Dataset dataset = testFile.createScalarDS
            ("2D 32-bit integer 20x10", root, dtype, dims2D, null, null, 0, dataIn);

        // close file resource
        testFile.close();
    }

}