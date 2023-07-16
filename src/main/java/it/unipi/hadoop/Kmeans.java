package it.unipi.hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class Kmeans {
    public static class KmeansMapper extends Mapper<Object, Text, IntWritable, Point>{
        private Point[] centroids;
        private Point[] combiner;
        private int[] counter;

        @Override
        protected void setup(Context context) throws IllegalArgumentException {

            // recover the config from the config
            String[] centroidConfig = context.getConfiguration().getStrings("centroids", "");
            int numDimensions = context.getConfiguration().getInt("dimensions", 0);

            if (numDimensions == 0) {
                throw new IllegalArgumentException("The centroids have no dimensions");
            }

            centroids = new Point[centroidConfig.length/numDimensions];
            combiner = new Point[centroidConfig.length/numDimensions];
            counter = new int[centroidConfig.length/numDimensions];

            for (int i = 0; i < centroidConfig.length/numDimensions; i++) {
                if (centroidConfig[i].isEmpty()) {
                    throw new IllegalArgumentException("Centroid is empty");
                }
                String centroid = "";
                //centroids[i] = Point.createPoint(centroidConfig[2*i] + ',' + centroidConfig[(2*i)+1]);
                for (int j = 0; j < numDimensions; j++) {
                    centroid = centroid.concat(centroidConfig[(2*i) +j] + ",");
                }
                centroids[i] = Point.createPoint(centroid.substring(0, centroid.length()-1));
            }

            if (centroids.length == 0) {
                throw new IllegalArgumentException("Centroid without coordinates");
            }
        }

        // Map method
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Point p = Point.createPoint(value.toString());

            // Check if point is null
            if (p.getSize() == 0) {
                throw new IllegalArgumentException("Point in MAP has size 0");
            }

            // Find the nearest centroid for the data point
            int index = p.nearest(centroids);

            if (combiner[index] == null){
                combiner[index] = p;
            }
            else {
                Point partial = combiner[index];
                combiner[index] = partial.sumPoints(p);
            }
            counter[index]++;
            // Emit the nearest centroid index and data point
            //context.write(new IntWritable(index), p);
        }

        @Override
        protected void cleanup(Mapper<Object, Text, IntWritable, Point>.Context context) throws IOException, InterruptedException {
            super.cleanup(context);
            for (int i = 0; i < combiner.length; i++) {
                if (combiner[i].getWeight() != counter[i]){
                    throw new IllegalArgumentException("IL PESO È SBAGLIATO");
                }
                context.write(new IntWritable(i), combiner[i]);
            }

        }
    }
    public static class KmeansReducer extends Reducer<IntWritable, Point, IntWritable, Point> {

        // Reduce method
        @Override
        protected void reduce(IntWritable key, Iterable<Point> cluster, Context context)
                throws IOException, InterruptedException {

            // set the new centroid as the average of the cluster
            Point newCentroid = Point.computeCentroid(cluster);
            context.write(key, newCentroid);

            //context.write(new IntWritable((int) Math.round(Math.random()*64)), newCentroid);
        }
    }



}