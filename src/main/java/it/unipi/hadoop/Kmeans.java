package it.unipi.hadoop;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Kmeans {
    public static class KmeansMapper extends Mapper<Object, Text, IntWritable, Point>{
        private Point[] centroids;

        @Override
        protected void setup(Context context) throws IllegalArgumentException {

            // recover the config from the config
            String[] centroidConfig = context.getConfiguration().getStrings("centroids");

            centroids = new Point[centroidConfig.length];
            for (int i = 0; i < centroidConfig.length; i++) {
                if (centroidConfig[i].isEmpty()) {
                    throw new IllegalArgumentException("Centroid is empty");
                }
                centroids[i] = Point.createPoint(centroidConfig[i]);
            }

            if (centroids.length == 0) {
                throw new IllegalArgumentException("Centroid without coordinates");
            }
        }

        // Map method
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Point p = Point.createPoint(value.toString());

            // Check if point is null
            if (p.getSize() == 0) {
                throw new IllegalArgumentException("Point in MAP has size 0");
            }

            double[] coord = p.getCoordinates();
            if (coord.length == 0){
                throw new IllegalArgumentException("Il punto creato non ha coordinate");
            }

            // Find the nearest centroid for the data point
            int index = p.nearest(centroids);

            // Emit the nearest centroid index and data point
            context.write(new IntWritable(index), p);
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
        }
    }



}