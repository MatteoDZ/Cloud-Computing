package it.unipi.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

public class Main {

    //TODO: CAPIRE DOVE LI HANNO TROVATI

    private static void log(String x){
        System.out.println("[\033[1;96mKMEANS\033[0m] " + x);
    }

    private static void logErr(String x){
        System.out.println("[\033[1;91mKMEANS\033[0m] " + x);
    }

    // extractResult returns an array of point which are the result of the last
    // hadoop run
    public static Point[] extractResult(Configuration conf, Path output, int K) throws IOException {

        // extract the hadoop's run outputs
        FileSystem fs = FileSystem.get(conf);
        Point[] newcentroids = new Point[K];

        for (FileStatus file : fs.listStatus(output)) {
            // if not a file skip it
            if (!file.isFile()) continue;

            // skip the success file
            if (file.getPath().getName().endsWith("_SUCCESS")) {
                continue;
            }

            // Open an input stream for reading the file
            Path filePath = file.getPath();
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(filePath)));

            // each file should have one or more line formatted like this
            // ```
            // key1 1.01;23.31;-12
            // ```

            br.lines().forEach(line -> {
                String[] splitted = line.split("\t");
                if (splitted.length != 2){
                    return;
                }
                newcentroids[Integer.parseInt(splitted[0])] = Point.parsePoint(splitted[1]);
            });

            // Close the input stream
            br.close();
        }

        return newcentroids;
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        PrintWriter dump = new PrintWriter(new BufferedWriter(new FileWriter("hadoop_out.csv")));
        PrintWriter dump_stats = new PrintWriter(new BufferedWriter(new FileWriter("hadoop_out.stats")));

        List<String> lines = Files.readAllLines(Paths.get(args[0]));

        Point[] centroids = lines.stream().map(Point::createPoint).toArray(Point[]::new);

        // Constants
        final int maxIter = 100;
        final double tolerance = 0.00001;
        final int c_length = centroids.length;

        Instant start = Instant.now();
        int iteration = 0;

        while(iteration < maxIter){

            Configuration config = new Configuration();

            String[] centroidsValue = new String[c_length];
            for (int i = 0; i < c_length; i++) {
                centroidsValue[i] = centroids[i].toString();
            }

            config.setStrings("centroids", centroidsValue);

            Path input = new Path(args[1]);
            Path output = new Path(args[2]);

            Job job = Job.getInstance(config);

            job.setJarByClass(Kmeans.class);

            job.setMapperClass(Kmeans.KmeansMapper.class);
            job.setCombinerClass(Kmeans.KmeansReducer.class);
            job.setReducerClass(Kmeans.KmeansReducer.class);
            job.setNumReduceTasks(c_length);

            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Point.class);

            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(Point.class);

            job.setMaxReduceAttempts(c_length);

            FileInputFormat.addInputPath(job, input);
            FileOutputFormat.setOutputPath(job, output);

            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            boolean end = job.waitForCompletion(true);
            if(!end) {
                dump_stats.close();
                dump.close();
                // Errors
                logErr("Error during iteration " + iteration);
                System.exit(1);
            }

            // read out the output
            Point[] newcentroids = extractResult(config, output, c_length);

            // get the distance between the new and the last centroids
            double distance = 0.0;
            for (int j = 0; j < c_length; j++) {
                distance+=newcentroids[j].distance(centroids[j]);
            }
            distance/=c_length;

            log("Current distance: " + distance);

            // if it's less then the stoppingTreshold exit gracefully
            if(distance < tolerance){
                log("StoppingTreshold reached");
                break;
            }

            // if not do another iteration with the new centroids

            for (int i = 0; i < centroids.length; i++) {
                if (newcentroids[i] != null){
                    centroids[i] = newcentroids[i];
                }
            }

            iteration++;
        }

        Duration execTime = Duration.between(start, Instant.now());

        for (Point centroid : centroids) {
            dump.println(centroid.toString());
        }
        dump.close();

        dump_stats.println("Execution Time: " + execTime.toMillis() + "ms");
        dump_stats.println("Number of Iterations: " + (iteration + 1));
        dump_stats.close();


        // Numero di reducer = numero di cluster o comunque logica simile
    }
}
