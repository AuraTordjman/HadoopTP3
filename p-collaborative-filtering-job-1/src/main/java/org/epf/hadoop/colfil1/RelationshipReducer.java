package org.epf.hadoop.colfil1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class RelationshipReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // Utiliser un ensemble pour éviter les doublons
        Set<String> uniqueRelations = new HashSet<>();

        for (Text value : values) {
            uniqueRelations.add(value.toString()); // Ajouter uniquement l'identifiant
        }

        // Construire une liste des relations uniques
        StringBuilder friendsList = new StringBuilder();
        for (String relation : uniqueRelations) {
            if (friendsList.length() > 0) {
                friendsList.append(", "); // Séparateur entre les relations
            }
            friendsList.append(relation);
        }

        // Émettre le résultat formaté
        context.write(key, new Text(friendsList.toString()));
    }

}
