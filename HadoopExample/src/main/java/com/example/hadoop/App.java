package com.example.hadoop;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class App {
  public static void main(String[] args) throws Exception {
    // Configuration de Hadoop
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Simple MapReduce Job");

    // Définir la classe principale du Job
    job.setJarByClass(App.class);

    // Définir les classes Mapper et Reducer
    job.setMapperClass(SimpleMapper.class);
    job.setReducerClass(SimpleReducer.class);

    // Définir les types de sortie
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(Text.class);

    // Définir les chemins d'entrée et de sortie (à modifier selon vos besoins)
    FileInputFormat.addInputPath(job, new Path("/input"));
    FileOutputFormat.setOutputPath(job, new Path("/output"));

    // Soumettre le job
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}