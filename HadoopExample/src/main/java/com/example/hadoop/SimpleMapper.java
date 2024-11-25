package com.example.hadoop;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
public class SimpleMapper extends Mapper<LongWritable, Text, LongWritable, Text>{
    /**
     * La méthode map lit une ligne et émet une paire (clé, valeur).
     *
     * @param key La clé d'entrée (numéro de ligne dans le fichier HDFS).
     * @param value La valeur d'entrée (le contenu de la ligne).
     * @param context Le contexte du Mapper pour émettre les paires (clé, valeur).
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Émettre la paire (numéro de ligne, contenu de la ligne)
        context.write(key, value);
    }
}