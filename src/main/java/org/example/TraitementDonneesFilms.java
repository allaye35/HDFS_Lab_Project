package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TraitementDonneesFilms {

    public static class MonMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text mot = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            mot.set(value);
            context.write(new Text(String.valueOf(key.get())), value);
        }
    }

    public static class MonReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            for (Text val : values) {
                sb.append(val.toString()).append(" ");
            }
            context.write(key, new Text(sb.toString()));
        }
    }

    // Mapper pour extraire movieID et le nom du film
    public static class MapperFilm extends Mapper<LongWritable, Text, Text, Text> {
        private Text movieID = new Text();
        private Text movieName = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");
            if (fields.length > 1) {
                movieID.set(fields[0]);
                movieName.set(fields[1]);
                context.write(movieID, movieName);
            }
        }
    }

    // Mapper pour extraire userID, movieID, et la note
    public static class MapperNote extends Mapper<LongWritable, Text, Text, Text> {
        private Text userID = new Text();
        private Text movieRating = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");
            if (fields.length > 2) {
                userID.set(fields[0]);
                movieRating.set(fields[1] + "," + fields[2]);
                context.write(userID, movieRating);
            }
        }
    }

    // Writable personnalisé pour transporter les valeurs (movieID, rating)
    public static class MovieRatingWritable implements Writable {
        private String movieID;
        private double rating;

        public MovieRatingWritable() {}

        public MovieRatingWritable(String movieID, double rating) {
            this.movieID = movieID;
            this.rating = rating;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeUTF(movieID);
            out.writeDouble(rating);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            movieID = in.readUTF();
            rating = in.readDouble();
        }

        public String getMovieID() {
            return movieID;
        }

        public double getRating() {
            return rating;
        }

        @Override
        public String toString() {
            return movieID + "," + rating;
        }
    }

    // Reducer pour trouver le film le mieux noté par userID
    public static class ReducerMeilleurFilm extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String meilleurFilm = "";
            double meilleureNote = Double.MIN_VALUE;

            for (Text val : values) {
                String[] parts = val.toString().split(",");
                if (parts.length == 2) {
                    String movieID = parts[0];
                    double rating = Double.parseDouble(parts[1]);
                    if (rating > meilleureNote) {
                        meilleureNote = rating;
                        meilleurFilm = movieID;
                    }
                }
            }
            context.write(key, new Text(meilleurFilm + "," + meilleureNote));
        }
    }

    // Reducer pour trouver le film le mieux noté par genre
    public static class ReducerMeilleurFilmParGenre extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String meilleurFilm = "";
            double meilleureNote = Double.MIN_VALUE;

            for (Text val : values) {
                String[] parts = val.toString().split(",");
                if (parts.length == 2) {
                    String movieName = parts[0];
                    double rating = Double.parseDouble(parts[1]);
                    if (rating > meilleureNote) {
                        meilleureNote = rating;
                        meilleurFilm = movieName;
                    }
                }
            }
            context.write(key, new Text(meilleurFilm + "," + meilleureNote));
        }
    }

    // Reducer pour trouver le tag le mieux noté
    public static class ReducerTagLeMieuxNote extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String meilleurTag = "";
            double meilleureNote = Double.MIN_VALUE;

            for (Text val : values) {
                String[] parts = val.toString().split(",");
                if (parts.length == 2) {
                    String tag = parts[0];
                    double rating = Double.parseDouble(parts[1]);
                    if (rating > meilleureNote) {
                        meilleureNote = rating;
                        meilleurTag = tag;
                    }
                }
            }
            context.write(key, new Text(meilleurTag + "," + meilleureNote));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // Job pour trouver le nom du film le mieux noté par utilisateur (opération de jointure)
        Job job1 = Job.getInstance(conf, "trouver le film le mieux noté par utilisateur");
        job1.setJarByClass(TraitementDonneesFilms.class);
        MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, MapperFilm.class);
        MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, MapperNote.class);
        job1.setReducerClass(ReducerMeilleurFilm.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job1, new Path(args[2]));
        job1.waitForCompletion(true);

        // Job pour trouver le film le mieux noté par genre
        Job job2 = Job.getInstance(conf, "trouver le film le mieux noté par genre");
        job2.setJarByClass(TraitementDonneesFilms.class);
        job2.setMapperClass(MonMapper.class);
        job2.setReducerClass(ReducerMeilleurFilmParGenre.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path(args[3]));
        FileOutputFormat.setOutputPath(job2, new Path(args[4]));
        job2.waitForCompletion(true);

        // Job pour trouver le tag le mieux noté
        Job job3 = Job.getInstance(conf, "trouver le tag le mieux noté");
        job3.setJarByClass(TraitementDonneesFilms.class);
        job3.setMapperClass(MonMapper.class);
        job3.setReducerClass(ReducerTagLeMieuxNote.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job3, new Path(args[5]));
        FileOutputFormat.setOutputPath(job3, new Path(args[6]));
        System.exit(job3.waitForCompletion(true) ? 0 : 1);
    }
}

// Deuxième série d'exercices :
// Un traitement plus avancé peut nécessiter plusieurs jobs Hadoop, quelques astuces sur les clés comme la concaténation des valeurs pour créer de nouvelles clés, ou l'utilisation des résultats des programmes précédents.
// Exemples :
// 1. Chaîner plusieurs jobs MapReduce pour effectuer une analyse plus complexe.
// 2. Concaténer des valeurs telles que userID et movieID pour créer des clés composites pour des jointures ou filtrages avancés.
// 3. Utiliser la sortie d'un job Hadoop comme entrée pour un autre job afin de réaliser un traitement itératif des données.
// 4. Implémenter un tri secondaire pour trier les notes des films pour chaque utilisateur.
// 5. Utiliser des classes Writable personnalisées pour gérer des structures de données complexes dans les phases map et reduce.
// 6. Implémenter un job pour trouver les utilisateurs ayant les préférences de films les plus similaires en comparant leurs évaluations.
// 7. Trouver le nom du film le mieux noté par userID. Pour cela, vous devez réaliser une opération de jointure pour extraire le nom du film à partir du fichier des films, en fonction de movieID. Dans ce cas, le plus simple est de simuler une jointure dans la phase de réduction en utilisant les sorties de deux classes Mapper, chacune traitant un fichier d'entrée spécifique.
// 8. Trouver le film le mieux noté par genre.
// 9. Trouver le tag le mieux noté.
