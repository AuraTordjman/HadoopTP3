package org.epf.hadoop.colfil2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class RelationshipReducer extends Reducer<UserPair, Text, UserPair, Text> {
    @Override
    protected void reduce(UserPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Set<String> commonFriends = new HashSet<>();
        boolean isDirect = false;

        for (Text value : values) {
            if (value.toString().equals("DIRECT")) {
                isDirect = true;
            } else {
                commonFriends.add(value.toString());
            }
        }

        // Émettre uniquement si ce n'est pas une relation directe
        if (!isDirect && !commonFriends.isEmpty()) {
            context.write(key, new Text(String.valueOf(commonFriends.size())));
        }
    }
}
