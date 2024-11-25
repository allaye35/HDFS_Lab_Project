package org.example;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;

public class MyApp {
    public static class Runner extends Configured implements Tool {
        @Override
        public int run(String[] args) throws Exception {
            if (args.length < 2) {
                System.err.println("Usage: MyApp <local_folder> <hdfs_dest>");
                return -1;
            }

            String localFolder = args[0];
            String hdfsDest = args[1];

            // Initialiser la configuration Hadoop
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);

            // Parcourir les fichiers dans le dossier local
            File folder = new File(localFolder);
            File[] files = folder.listFiles();
            if (files == null) {
                System.err.println("No files found in " + localFolder);
                return -1;
            }

            // Téléverser chaque fichier dans HDFS
            for (File file : files) {
                if (file.isFile()) {
                    Path src = new Path(file.getAbsolutePath());
                    Path dest = new Path(hdfsDest + "/" + file.getName());
                    System.out.println("Uploading: " + file.getName());
                    fs.copyFromLocalFile(src, dest);
                }
            }

            System.out.println("All files uploaded successfully.");
            return 0;
        }
    }

    public static void main(String[] args) throws Exception {
        int returnCode = ToolRunner.run(new MyApp.Runner(), args);
        System.exit(returnCode);
    }
}
