package it.unipi.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class Point implements Writable {

    private int weight = 1; //Constant, since all count the same
    private double[] coordinates;
    private int size;
    public Point() {

    }

    public Point(int weight, double[] coordinates){
        this.weight = weight;
        this.coordinates = coordinates;
        this.size = coordinates.length;
    }

    // Create new point from string, used in main to convert from csv to java structure
    public static Point createPoint(String value) throws IllegalArgumentException {

        List<Double> coordinates = new ArrayList<>();
        StringTokenizer tokenizer = new StringTokenizer(value, ",");
        while (tokenizer.hasMoreTokens()) {
            coordinates.add(Double.parseDouble(tokenizer.nextToken()));
        }

        if (coordinates.isEmpty()) {
            throw new IllegalArgumentException("No coordinates found.");
        }

        double[] coordinatesArray = new double[coordinates.size()];
        for (int i = 0; i < coordinates.size(); i++) {
            coordinatesArray[i] = coordinates.get(i);
        }

        return new Point(1, coordinatesArray);
    }

    public Point sumPoints(Point p) {
        int weight = this.weight + p.weight;
        double[] coords = new double[size];
        for (int i = 0; i < size; i++) {
            coords[i] = this.coordinates[i] + p.coordinates[i];
        }
        return new Point(weight, coords);
    }

    // Return the distance between this Point and Point p
    public double distance(Point p) throws IllegalArgumentException {

        // Using Euclidian distance
        double squaredSum = 0.0;
        for (int i = 0; i < coordinates.length; i++) {
            squaredSum += Math.pow(this.coordinates[i] - p.coordinates[i], 2);
        }
        return Math.sqrt(squaredSum);
    }

    // Return the nearest point
    public int nearest(Point[] points) throws IllegalArgumentException {

        if (points.length == 0){
            throw new IllegalArgumentException();
        }

        int nearest = -1;
        double min_distance = Double.MAX_VALUE;
        int len = points.length;

        for(int i = 0; i < len; i++){
            double temp_distance = this.distance(points[i]);
            if (temp_distance < min_distance){
                nearest = i;
                min_distance = temp_distance;
            }
        }
        return nearest;
    }

    public static Point computeCentroid(Iterable<Point> points) throws IllegalArgumentException {

        Iterator<Point> iterator = points.iterator();
        if (!iterator.hasNext()) {
            throw new IllegalArgumentException("Iterable has no elements.");
        }

        // Get size from the first point
        Point firstPoint = iterator.next();
        // If we use firstPoint.size instead of firstPoint.coordinates.length everything stops working
        final int size = firstPoint.coordinates.length;
        double[] centerCoordinates = new double[firstPoint.coordinates.length];

        for (int i = 0; i < firstPoint.coordinates.length; i++) {
            centerCoordinates[i] = firstPoint.coordinates[i];
        }
        int totalWeight = firstPoint.weight;

        // Sum of all points and weights
        while (iterator.hasNext()) {
            Point point = iterator.next();
            for (int i = 0; i < size; i++) {
                centerCoordinates[i] += point.coordinates[i];
            }
            totalWeight += point.weight;
        }

        // Average using weight
        for (int i = 0; i < size; i++) {
            centerCoordinates[i] /= totalWeight;
            if (centerCoordinates[i] > 15.0 || centerCoordinates[i] < -15.0){
                throw new IllegalArgumentException("Coordinate too big");
            }
        }

        return new Point(1, centerCoordinates);
    }

    public double[] getCoordinates() {
        return coordinates;
    }

    public int getSize() {
        return size;
    }

    public int getWeight() {return weight;}

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(coordinates.length);
        for (double coordinate : this.coordinates) {
            dataOutput.writeDouble(coordinate);
        }
        dataOutput.writeInt(this.weight);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        int length = dataInput.readInt();
        this.coordinates = new double[length];
        for (int i = 0; i < length; i++) {
            coordinates[i] = dataInput.readDouble();
        }
        this.weight = dataInput.readInt();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        String prefix = "";
        for (double d : this.coordinates) {
            sb.append(prefix);
            prefix = ",";
            sb.append(d);
        }
        return sb.toString();
    }
}
