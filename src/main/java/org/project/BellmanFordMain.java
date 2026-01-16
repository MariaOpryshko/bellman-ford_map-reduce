package org.project;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class BellmanFordMain extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Need args: <input> <output> <num vertices> <head vertex name>");
            return -1;
        }

        String inputPathStr = args[0];
        String outputPathStr = args[1];
        int iterations = Integer.parseInt(args[2]) - 1;
        String sourceVertex = args[3];


        Configuration conf = getConf();
        conf.set("source.vertex.id", sourceVertex);

        for (int i = 0; i < iterations; i++) {
            Job job = Job.getInstance(conf, "Bellman-Ford Iteration " + i);

            job.setJarByClass(BellmanFordMain.class);

            job.setMapperClass(BellmanFordMapper.class);
            job.setReducerClass(BellmanFordReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Vertex.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path(inputPathStr));

            String outputPathIterStr = outputPathStr + "iter" + i;
            Path outputPathIter = new Path(outputPathIterStr);
            // clean old output files if they exist
            FileSystem fs = FileSystem.get(conf);
            if (fs.exists(outputPathIter)) {
                fs.delete(outputPathIter, true);
            }
            FileOutputFormat.setOutputPath(job, outputPathIter);

            job.waitForCompletion(true);
            inputPathStr = outputPathIterStr;
        }

        return 0;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new BellmanFordMain(), args);
        System.exit(exitCode);
    }
}
