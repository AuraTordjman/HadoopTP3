package org.epf.hadoop.colfil1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ColFilJob1 {
    public static void main(String[] args) throws Exception {
        // Vérifier les arguments

        // Afficher les arguments reçus
        System.out.println("Arguments received:");
        for (String arg : args) {
            System.out.println(arg);
        }
        // Créer une configuration Hadoop
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Collaborative Filtering Job 1");

        // Définir la classe principale
        job.setJarByClass(ColFilJob1.class);

        // Définir les classes Mapper, Reducer, et InputFormat
        job.setMapperClass(RelationshipMapper.class);
        job.setReducerClass(RelationshipReducer.class);
        job.setInputFormatClass(RelationshipInputFormat.class);

        // Définir les types de données de sortie pour Mapper et Reducer
        job.setMapOutputKeyClass(Text.class);       // Clé en sortie du Mapper
        job.setMapOutputValueClass(Text.class);     // Valeur en sortie du Mapper
        job.setOutputKeyClass(Text.class);          // Clé en sortie du Reducer
        job.setOutputValueClass(Text.class);        // Valeur en sortie du Reducer

        // Configurer le nombre de Reducers (2 comme demandé)
        job.setNumReduceTasks(2);

        // Définir les chemins d'entrée et de sortie (passés en arguments)
        FileInputFormat.addInputPath(job, new Path(args[1])); // Chemin des données d'entrée
        FileOutputFormat.setOutputPath(job, new Path(args[2])); // Chemin des résultats
        System.out.println("Input Path: " + args[1]);
        System.out.println("Output Path: " + args[2]);

        // Lancer le job et attendre la fin
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
