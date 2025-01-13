package org.epf.hadoop.colfil3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class RecommenderReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        System.out.println("Reducer Input Key: " + key.toString());

        Map<String, Integer> recommendations = new HashMap<>();

        for (Text value : values) {
            System.out.println("Reducer Input Value: " + value.toString());
            String[] parts = value.toString().split(",");
            if (parts.length != 2) {
                System.out.println("Invalid Reducer Input Value: " + value.toString());
                continue;
            }

            String user = parts[0];
            int commonRelations = Integer.parseInt(parts[1]);

            recommendations.put(user, recommendations.getOrDefault(user, 0) + commonRelations);
        }


        List<Map.Entry<String, Integer>> sortedRecommendations = new ArrayList<>(recommendations.entrySet());
        sortedRecommendations.sort((a, b) -> b.getValue().compareTo(a.getValue()));

        // top 5 recommendations
        List<String> topRecommendations = new ArrayList<>();
        for (int i = 0; i < Math.min(5, sortedRecommendations.size()); i++) {
            Map.Entry<String, Integer> entry = sortedRecommendations.get(i);
            topRecommendations.add(entry.getKey() + "(" + entry.getValue() + ")");
        }

        System.out.println("Reducer Output for " + key.toString() + ": " + String.join(", ", topRecommendations));
        context.write(key, new Text(String.join(", ", topRecommendations)));
    }
}
