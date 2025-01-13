package org.epf.hadoop.colfil3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ColFilJob3 {
    public static void main(String[] args) throws Exception {


        Configuration conf = new Configuration();
        System.out.println("Initializing Job: Collaborative Filtering Job 3");
        System.out.println("Input Path: " + args[0]); // pour moi
        System.out.println("Output Path: " + args[1]);// pour moi
        System.out.println("Output Path: " + args[2]);// pour moi

        Job job = Job.getInstance(conf, "Collaborative Filtering Job 3");

        job.setJarByClass(ColFilJob3.class);
        job.setMapperClass(RecommenderMapper.class);
        job.setReducerClass(RecommenderReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.out.println("Input Path: " + args[0]); // pour moi
        System.out.println("Output Path: " + args[1]); // pour moi
        System.out.println("Input Path: " + args[2]); // pour moi

        System.out.println("Job Configuration Complete. Starting Job...");

        boolean success = job.waitForCompletion(true);
        System.out.println("Job Finished. Status: " + (success ? "SUCCESS" : "FAILURE"));
        System.exit(success ? 0 : 1);
    }
}
