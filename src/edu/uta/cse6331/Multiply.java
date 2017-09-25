package edu.uta.cse6331;

/**
 * Created by siddhantattri on 9/24/17.
 */
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Scanner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

class m_Matrix implements Writable {
    public int iRowNumber;
    public int jColumnNumber;
    public double value;

    m_Matrix (int i, int j, double v) {
        iRowNumber = i;
        jColumnNumber = j;
        value = v;
    }

    public void write (DataOutput output) throws IOException {
        output.writeInt(iRowNumber);
        output.writeInt(jColumnNumber);
        output.writeDouble(value);
    }

    public void readFields (DataInput input) throws IOException {
        iRowNumber = input.readInt();
        jColumnNumber = input.readInt();
        value = input.readDouble();
    }

}

class n_Matrix implements Writable {
    public int jRowNumber;
    public int kColumnNumber;
    public double value;

    n_Matrix (int j, int k, double v) {
        jRowNumber = j;
        kColumnNumber = k;
        value = v;
    }

    public void write (DataOutput output) throws IOException {
        output.writeInt(jRowNumber);
        output.writeInt(kColumnNumber);
        output.writeDouble(value);
    }

    public void readFields (DataInput input) throws IOException {
        jRowNumber = input.readInt();
        kColumnNumber = input.readInt();
        value = input.readDouble();
    }
}

class IntermediateMatrix implements Writable {
    public short tag;
    public m_Matrix mMatrix;
    public n_Matrix nMatrix;

    IntermediateMatrix () {}
    IntermediateMatrix (m_Matrix m ) { tag = 0; mMatrix = m; }
    IntermediateMatrix (n_Matrix n ) { tag = 1; nMatrix = n; }

    public void write (DataOutput output) throws IOException {
        output.writeShort(tag);
        if (tag == 0)
            mMatrix.write(output);
        nMatrix.write(output);
    }

    public  void  readFields (DataInput input) throws IOException {

    }
}

class IntermediateResult implements Writable {
    public double value;

    IntermediateResult() {}
    IntermediateResult (double v) {
        value = v;
    }

    public void write (DataOutput output) throws IOException {
        output.writeDouble(value);
    }
    public void readFields (DataInput input) throws IOException {
        value = input.readDouble();
    }
}

class Pairing implements WritableComparable<Pairing> {

    public int i;
    public int k;

    Pairing (int iVal, int kVal){
        i = iVal; k = kVal;
    }

    public void write (DataOutput output) throws IOException {
        output.writeInt(i);
        output.writeInt(k);
    }

    public void readFields (DataInput input) throws IOException {
        i = input.readInt();
        k = input.readInt();
    }

    public int compareTo(Pairing pairValue) {
        
    }


}


public class Multiply {

    public static void main ( String[] args ) throws Exception {


    }
}
