package it.unipi.hadoop;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.mapreduce.Job;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
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
            // key1;1.01,23.31,-12
            // ```

            br.lines().forEach(line -> {
                String[] splitted = line.split("\t");
                System.out.println(splitted.length);
                if (splitted.length != 2){
                    return;
                }
                newcentroids[Integer.parseInt(splitted[0])] = Point.createPoint(splitted[1]);
            });

            // Close the input stream
            br.close();
        }

        return newcentroids;
    }

    private static ArrayList<Point> initializeCentroids(Configuration conf, String inputPath){
        ArrayList<Point> initCentroids = new ArrayList<>();

        for(int i = 0; i < conf.getInt("num_centroids", 0); i++){
            List<String> lines = new ArrayList<>();
            try {
                lines = Files.readAllLines(Paths.get(inputPath));
            } catch (IOException e) {
                e.printStackTrace();
            }
            //Seleziona casualmente una delle righe del dataset
            int row = (int) (Math.random() * 100000);//conf.getInt("NUM_POINTS", 0));
            Point p = Point.createPoint(lines.get(row));
            initCentroids.add(p);
        }
        return initCentroids;
    }

    public static List<Point> initialize(ArrayList<Point> data, int k) {
        List<Point> centroids = new ArrayList<>();
        int rand = (int)(Math.random() * data.size());

        // Add a randomly selected data point to the list
        centroids.add(data.get(rand));

        // Compute remaining k - 1 centroids
        for (int c_id = 0; c_id < k - 1; c_id++) {
            List<Double> dist = new ArrayList<>();
            for (Point point : data) {
                double d = Double.MAX_VALUE;

                // Compute distance of 'point' from each of the previously selected centroid and store the minimum distance
                for (Point centroid : centroids) {
                    double temp_dist = point.distance(centroid);
                    d = Math.min(d, temp_dist);
                }
                dist.add(d);
            }
            // Select data point with maximum distance as our next centroid
            Point next_centroid = data.get(argmax(dist));
            centroids.add(next_centroid);
            dist.clear();
        }
        return centroids;
    }

    public static int argmax(List<Double> arr) {
        int maxIndex = 0;
        double maxVal = Double.MIN_VALUE;
        for (int i = 0; i < arr.size(); i++) {
            double val = arr.get(i);
            if (val > maxVal) {
                maxVal = val;
                maxIndex = i;
            }
        }
        return maxIndex;
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        PrintWriter dump = new PrintWriter(new BufferedWriter(new FileWriter("hadoop_out.csv")));
        PrintWriter dump_stats = new PrintWriter(new BufferedWriter(new FileWriter("hadoop_out.stats")));

        Path input = new Path(args[1]);

        Configuration config = new Configuration();

        // Constants
        final int maxIter = 20;
        final double tolerance = 0.1;
        final int c_length = Integer.parseInt(args[0]);
        int iteration = 0;

        System.out.println(c_length);

        config.setInt("num_centroids", c_length);
        //Point[] centroids = initializeCentroids(config, args[1]).toArray(new Point[c_length]);
        List<String> punti = Files.readAllLines(Paths.get(args[1]));
        ArrayList<Point> points = new ArrayList<>();
        for(String s : punti){
            points.add(Point.createPoint(s));
        }
        List<Point> centroids = initialize(points, c_length);

        Instant start = Instant.now();


        while(iteration < maxIter){

            String[] centroidsValue = new String[c_length];
            for(int i = 0; i < c_length; i++){
                centroidsValue[i] = centroids.get(i).toString();
                System.out.println(centroidsValue[i]);
            }


            config.setStrings("centroids", centroidsValue);

            Job job = Job.getInstance(config);

            job.setJarByClass(Kmeans.class);

            job.setMapperClass(Kmeans.KmeansMapper.class);
            //job.setCombinerClass(Kmeans.KmeansReducer.class);
            job.setReducerClass(Kmeans.KmeansReducer.class);
            job.setNumReduceTasks(c_length);

            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Point.class);

            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(Point.class);

            job.setMaxReduceAttempts(c_length);

            FileInputFormat.addInputPath(job, input);
            Path output_iter = new Path(args[2] + iteration);
            FileOutputFormat.setOutputPath(job,  output_iter);


            boolean end = job.waitForCompletion(true);
            if(!end) {
                dump_stats.close();
                dump.close();
                // Errors
                logErr("Error during iteration " + iteration);
                System.exit(1);
            }

            // read out the output
            Point[] newcentroids = extractResult(config, output_iter, c_length);

            // get the distance between the new and the last centroids
            double distance = 0.0;
            for (int j = 0; j < c_length; j++) {
                distance += newcentroids[j].distance(centroids.get(j));
            }
            distance/=c_length;

            log("Current distance: " + distance);

            // if it's less then the stoppingTreshold exit gracefully
            if(distance < tolerance){
                log("StoppingTreshold reached");
                break;
            }

            // if not do another iteration with the new centroids

            for (int i = 0; i < centroids.size(); i++) {
                if (newcentroids[i] != null){
                    centroids.set(i, newcentroids[i]);
                }
            }

            for (Point centroid : centroids) {
                dump.println(centroid.toString());
            }
            dump.println("");

            iteration++;
        }

        Duration execTime = Duration.between(start, Instant.now());
        dump.close();

        dump_stats.println("Execution Time: " + execTime.toMillis() + "ms");
        dump_stats.println("Number of Iterations: " + (iteration + 1));
        dump_stats.close();


        // Numero di reducer = numero di cluster o comunque logica simile
    }
}
