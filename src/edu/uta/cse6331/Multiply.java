package edu.uta.cse6331;

/**
 * Created by siddhantattri on 9/24/17.
 */

        import java.io.DataInput;
        import java.io.DataOutput;
        import java.io.IOException;
        import java.util.Scanner;
        import java.util.Vector;
        import org.apache.hadoop.fs.Path;
        import org.apache.hadoop.io.*;
        import org.apache.hadoop.mapreduce.*;
        import org.apache.hadoop.mapreduce.lib.input.*;
        import org.apache.hadoop.mapreduce.lib.output.*;

class N_Matrix implements Writable {
    public int jRowNumber;
    public int kColumnNumber;
    public double val;

    N_Matrix () {}

    N_Matrix ( int j, int k, double v ) {
        jRowNumber = j;
        kColumnNumber = k;
        val = v;
    }

    public void write ( DataOutput out ) throws IOException {
        out.writeInt(jRowNumber);
        out.writeInt(kColumnNumber);
        out.writeDouble(val);
    }

    public void readFields ( DataInput in ) throws IOException {
        jRowNumber = in.readInt();
        kColumnNumber = in.readInt();
        val = in.readDouble();
    }
}

class M_Matrix implements Writable {
    public int iRowNumber;
    public int jColumnNumber;
    public double val;

    M_Matrix () {}

    M_Matrix ( int i, int j, double v ) {
        iRowNumber = i;
        jColumnNumber = j;
        val = v;
    }

    public void write ( DataOutput out ) throws IOException {
        out.writeInt(jColumnNumber);
        out.writeInt(iRowNumber);
        out.writeDouble(val);
    }

    public void readFields ( DataInput in ) throws IOException {
        jColumnNumber = in.readInt();
        iRowNumber = in.readInt();
        val = in.readDouble();
    }
}



class InterResult implements Writable {
    public double value;

    InterResult () {}

    InterResult (double v ) {
        value = v;
    }

    public void write ( DataOutput out ) throws IOException {
        out.writeDouble(value);
    }


    public void readFields ( DataInput in ) throws IOException {
        value = in.readDouble();
    }

}

class Pair implements WritableComparable<Pair> {
    public int i;
    public int k;

    Pair () {}

    Pair (int iValue,int kValue ) {
        i = iValue; k = kValue;
    }

    public void write ( DataOutput out ) throws IOException {
        out.writeInt(i);
        out.writeInt(k);
    }


    public void readFields ( DataInput in ) throws IOException {
        i = in.readInt();
        k = in.readInt();
    }

    @Override
    public int compareTo(Pair pVal) {
        int iValue = this.i;
        int kValue = this.k;
        int piVal = pVal.i;
        int pkVal = pVal.k;
        int result = 0;
        if(iValue == piVal){
            if(kValue == pkVal){
                result = 0;
            }else if(kValue < pkVal){
                result = -1;
            }else{
                result = 1;
            }
        }else if(iValue < piVal){
            result = -1;
        }else if (iValue > piVal){
            result = 1;
        }
        return result;
    }

    public String toString () { return i+" , "+k; }
}

class InterMatrix implements Writable {
    public short flag;
    public M_Matrix mMatrix;
    public N_Matrix nMatrix;

    InterMatrix () {}
    InterMatrix ( M_Matrix m ) { flag = 0; mMatrix = m; }
    InterMatrix ( N_Matrix n ) { flag = 1; nMatrix = n; }

    public void write ( DataOutput out ) throws IOException {
        out.writeShort(flag);
        if (flag==0)
            mMatrix.write(out);
        else nMatrix.write(out);
    }

    public void readFields ( DataInput in ) throws IOException {
        flag = in.readShort();
        if (flag==0) {
            mMatrix = new M_Matrix();
            mMatrix.readFields(in);
        } else {
            nMatrix = new N_Matrix();
            nMatrix.readFields(in);
        }
    }
}




public class Multiply {

    public static class N_MatrixMapper extends Mapper<Object,Text,IntWritable,InterMatrix > {
        @Override
        public void map ( Object key, Text value, Context context )
                throws IOException, InterruptedException {
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
            N_Matrix n = new N_Matrix(s.nextInt(),s.nextInt(),s.nextDouble());
            context.write(new IntWritable(n.jRowNumber),new InterMatrix(n));
            s.close();
        }
    }
    public static class M_MatrixMapper extends Mapper<Object,Text,IntWritable,InterMatrix > {
        @Override
        public void map ( Object key, Text value, Context context )
                throws IOException, InterruptedException {
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
            M_Matrix m = new M_Matrix(s.nextInt(),s.nextInt(),s.nextDouble());
            context.write(new IntWritable(m.jColumnNumber),new InterMatrix(m));
            s.close();
        }
    }



    public static class ResultReducer extends Reducer<IntWritable,InterMatrix,Pair,InterResult> {
        static Vector<M_Matrix> m_Matrix = new Vector<M_Matrix>();
        static Vector<N_Matrix> n_Matrix = new Vector<N_Matrix>();
        public void reduce ( IntWritable key, Iterable<InterMatrix> values, Context context )
                throws IOException, InterruptedException {
            m_Matrix.clear();
            n_Matrix.clear();
            for (InterMatrix v: values)
                if (v.flag == 0)
                    m_Matrix.add(v.mMatrix);
                else n_Matrix.add(v.nMatrix);
            for ( M_Matrix m: m_Matrix )
            {
                for ( N_Matrix n: n_Matrix )
                {
                    context.write(new Pair(m.iRowNumber,n.kColumnNumber),new InterResult(m.val*n.val));
                }
            }
        }
    }

    public static class SecondMapper extends Mapper<Object,InterResult,Object,InterResult > {
        @Override
        public void map ( Object key, InterResult value, Context context )
                throws IOException, InterruptedException {
            context.write(key,value);
        }
    }

    public static class SecondReducer extends Reducer<Pair,InterResult,Pair,String> {
        @Override
        public void reduce ( Pair key, Iterable<InterResult> values, Context context )
                throws IOException, InterruptedException {
            double sum = 0.0;
            for (InterResult v: values) {
                sum += v.value;
            };
            context.write(key,","+sum);
        }
    }

    public static void main(String[] args) throws Exception{
        Job job1 = Job.getInstance();
        job1.setJobName("FirstJob");
        job1.setJarByClass(Multiply.class);
        job1.setOutputKeyClass(Pair.class);
        job1.setOutputValueClass(InterResult.class);
        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(InterMatrix.class);
        job1.setReducerClass(ResultReducer.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        job1.setNumReduceTasks(2);
        MultipleInputs.addInputPath(job1,new Path(args[0]),TextInputFormat.class,M_MatrixMapper.class);
        MultipleInputs.addInputPath(job1,new Path(args[1]),TextInputFormat.class,N_MatrixMapper.class);
        FileOutputFormat.setOutputPath(job1,new Path(args[2]));
        job1.waitForCompletion(true);
        Job job2 = Job.getInstance();
        job2.setJobName("SecondJob");
        job2.setJarByClass(Multiply.class);
        job2.setOutputKeyClass(Pair.class);
        job2.setOutputValueClass(String.class);
        job2.setMapOutputKeyClass(Pair.class);
        job2.setMapOutputValueClass(InterResult.class);
        job2.setReducerClass(SecondReducer.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        MultipleInputs.addInputPath(job2,new Path(args[2]),SequenceFileInputFormat.class,SecondMapper.class);
        FileOutputFormat.setOutputPath(job2,new Path(args[3]));
        job2.waitForCompletion(true);
    }

}