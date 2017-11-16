package edu.uta.cse6331;

/**
 * Created by siddhantattri on 10/7/17.
 */
        import java.io.DataInput;
        import java.io.DataOutput;
        import java.io.IOException;
        import java.util.Scanner;
        import java.util.Vector;

        import org.apache.hadoop.conf.Configuration;
        import org.apache.hadoop.conf.Configured;
        import org.apache.hadoop.fs.Path;
        import org.apache.hadoop.io.*;
        import org.apache.hadoop.mapreduce.*;
        import org.apache.hadoop.mapreduce.lib.input.*;
        import org.apache.hadoop.mapreduce.lib.output.*;
        import org.apache.hadoop.util.Tool;
        import org.apache.hadoop.util.ToolRunner;

class Vertex implements Writable {
    short tag;  // 0 for a graph vertex, 1 for a group number
    long group; // the group where this vertex belongs to
    long VID; // the vertex ID
    Vector<Long> adjacent = new Vector<Long>(); // the vertex neighbors

    Vertex () {}

    Vertex (short t,long g){
        tag = 1;
        group = g;
    }

    Vertex (short t,long g,long vId,Vector<Long> adj){
        tag = 0;
        group = g;
        VID = vId;
        adjacent = adj;
    }

    public void write ( DataOutput out ) throws IOException {
        out.writeShort(tag);
        if(tag == 1){
            out.writeLong(group);
        }
        else{
            out.writeLong(group);
            out.writeLong(VID);
            out.writeInt(adjacent.size());
            for(int i=0;i<adjacent.size();i++){
                out.writeLong(adjacent.get(i));
            }
        }
    }

    public void readFields ( DataInput in ) throws IOException {
        tag = in.readShort();
        if(tag == 1){
            group = in.readLong();
        }
        else{
            group = in.readLong();
            VID = in.readLong();
            int length = in.readInt();
            if(length > 0){
                adjacent.clear();
                for(int i=0;i<length;i++){
                    adjacent.addElement(in.readLong());
                }
            }
        }
    }

    public String toString () { return String.valueOf(VID); }
}


public class Graph extends Configured implements Tool{

    public static class GraphOneMapper extends Mapper<Object,Text,LongWritable,Vertex > {
        @Override
        public void map ( Object key, Text value, Context context )
                throws IOException, InterruptedException {
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
            long vid = s.nextLong();
            Vector<Long> adjacent = new Vector<Long>();
            while(s.hasNext()){   //parse the line to get the vertex VID and the adjacent vector
                adjacent.add(s.nextLong());
            }
            context.write(new LongWritable(vid),new Vertex((short)0,vid,vid,adjacent));
            s.close();
        }
    }

    public static class GraphTwoMapper extends Mapper<LongWritable,Vertex,LongWritable,Vertex > {
        @Override
        public void map ( LongWritable key, Vertex value, Context context )
                throws IOException, InterruptedException {
            context.write(new LongWritable(value.VID),value);
            if(value.adjacent != null){  // pass the graph topology
                for(Long n: value.adjacent){
                    context.write(new LongWritable(n), new Vertex((short)1,value.group));  // send the group # to the adjacent vertices
                }
            }
        }
    }

    public static class GraphTwoReducer extends Reducer<LongWritable,Vertex,LongWritable,Vertex> {
        @Override
        public void reduce ( LongWritable key, Iterable<Vertex> values, Context context )
                throws IOException, InterruptedException {
            long minValue = Long.MAX_VALUE;
            Vertex vertex = null;
            for ( Vertex v: values ){
                if (v.tag == 0)
                    vertex = v;
                minValue = min(minValue,v.group);  // found the vertex with vid
            }
            context.write(new LongWritable(minValue),new Vertex((short)0,minValue,vertex.VID,vertex.adjacent) ); // new group #
        }
    }

    public static class GraphThreeMapper extends Mapper<LongWritable,Vertex,LongWritable,IntWritable > {
        @Override
        public void map ( LongWritable key, Vertex value, Context context )
                throws IOException, InterruptedException {
            context.write(key,new IntWritable(1));
        }
    }

    public static class GraphThreeReducer extends Reducer<LongWritable,IntWritable,LongWritable,String> {
        long minValue = Long.MAX_VALUE;
        Vertex vertex;
        @Override
        public void reduce ( LongWritable key, Iterable<IntWritable> values, Context context )
                throws IOException, InterruptedException {
            int size = 0;
            for ( IntWritable v: values){
                size = size+v.get();
            }
            context.write(key,","+size);
        }
    }


    public static Long min(long minValue,long groupValue){
        if(minValue<groupValue)
            return minValue;
        return groupValue;
    }

    @Override
    public int run ( String [] args ) throws Exception {
        Configuration conf = getConf();
        Job job1 = Job.getInstance(conf);
        job1.setJobName("1stJob");
        job1.setJarByClass(Graph.class);
        job1.setOutputKeyClass(LongWritable.class);
        job1.setOutputValueClass(Vertex.class);
        job1.setMapOutputKeyClass(LongWritable.class);
        job1.setMapOutputValueClass(Vertex.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        job1.setNumReduceTasks(0);
        MultipleInputs.addInputPath(job1,new Path(args[0]),TextInputFormat.class,GraphOneMapper.class);
        FileOutputFormat.setOutputPath(job1,new Path(args[1]+"/f0"));
        job1.waitForCompletion(true);
        for(int i=0;i<5;i++){
            Job job2 = Job.getInstance(conf);
            job2.setJobName("2ndJob");
            job2.setJarByClass(Graph.class);
            job2.setOutputKeyClass(LongWritable.class);
            job2.setOutputValueClass(Vertex.class);
            job2.setMapOutputKeyClass(LongWritable.class);
            job2.setMapOutputValueClass(Vertex.class);
            job2.setReducerClass(GraphTwoReducer.class);
            job2.setOutputFormatClass(SequenceFileOutputFormat.class);
            MultipleInputs.addInputPath(job2,new Path(args[1]+"/f"+i),SequenceFileInputFormat.class,GraphTwoMapper.class);
            FileOutputFormat.setOutputPath(job2,new Path(args[1]+"/f"+(i+1)));
            job2.waitForCompletion(true);
        }
        Job job3 = Job.getInstance(conf);
        job3.setJobName("3rdJob");
        job3.setJarByClass(Graph.class);
        job3.setOutputKeyClass(LongWritable.class);
        job3.setOutputValueClass(String.class);
        job3.setMapOutputKeyClass(LongWritable.class);
        job3.setMapOutputValueClass(IntWritable.class);
        job3.setReducerClass(GraphThreeReducer.class);
        job3.setOutputFormatClass(TextOutputFormat.class);
        MultipleInputs.addInputPath(job3,new Path(args[1]+"/f5"),SequenceFileInputFormat.class,GraphThreeMapper.class);
        FileOutputFormat.setOutputPath(job3,new Path(args[2]));
        job3.waitForCompletion(true);
        return 0;
    }

    public static void main ( String[] args ) throws Exception {
        ToolRunner.run(new Configuration(),new Graph(),args);
    }
}