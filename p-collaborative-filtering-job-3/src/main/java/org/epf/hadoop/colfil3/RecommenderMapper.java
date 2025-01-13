package org.epf.hadoop.colfil3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RecommenderMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        System.out.println("Mapper Input Line: " + value.toString());


        String[] line = value.toString().split("\t");
        if (line.length != 2) {
            System.out.println("Invalid Input Line: " + value.toString());
            return;
        }

        String[] users = line[0].split(",");
        if (users.length != 2) {
            System.out.println("Invalid User Pair: " + line[0]);
            return;
        }

        String userA = users[0];
        String userB = users[1];
        String commonRelations = line[1];

        // Emit both directions
        context.write(new Text(userA), new Text(userB + "," + commonRelations));
        context.write(new Text(userB), new Text(userA + "," + commonRelations));

        System.out.println("Mapper Output: (" + userA + " -> " + userB + "," + commonRelations + ")"); // pour moi
        System.out.println("Mapper Output: (" + userB + " -> " + userA + "," + commonRelations + ")");// pour moi
    }
}
