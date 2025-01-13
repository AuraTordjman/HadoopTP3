package org.epf.hadoop.colfil2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ColFilJob2 {
    public static void main(String[] args) throws Exception {
        // Vérification des arguments
        if (args.length < 2) {
            System.err.println("Usage: ColFilJob2 <input path> <output path>");
            System.exit(-1);
        }

        System.out.println("Arguments received:");
        for (String arg : args) {
            System.out.println(arg);
        }

        // Configuration de Hadoop
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Collaborative Filtering Job 2");

        // Définir la classe principale du job
        job.setJarByClass(ColFilJob2.class);

        // Définir les classes Mapper et Reducer
        job.setMapperClass(RelationshipMapper.class);
        job.setReducerClass(RelationshipReducer.class);

        // Définir les types pour les sorties de Mapper et Reducer
        job.setMapOutputKeyClass(UserPair.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(UserPair.class);
        job.setOutputValueClass(Text.class);

        // Configurer le nombre de reducers
        job.setNumReduceTasks(2);

        // Définir les chemins d'entrée et de sortie
        FileInputFormat.addInputPath(job, new Path(args[1])); // Premier argument : chemin d'entrée
        FileOutputFormat.setOutputPath(job, new Path(args[2])); // Deuxième argument : chemin de sortie

        // Lancer le job et attendre la fin
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
