package it.unipi.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class Point implements Writable {

    private int weight = 1;
    private double[] coordinates;
    private int size;
    public Point() {

    }

    public Point(int weight, double[] coordinates){
        this.weight = weight;
        this.coordinates = coordinates;
        this.size = coordinates.length;
    }

    // Create new point from string
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

    // Return the distance between this Point and Point p
    public double distance(Point p) throws IllegalArgumentException {

        // Using Euclidian distance
        int len = p.coordinates.length;
        double squaredSum = 0.0;
        for (int i = 0; i < len; i++) {
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
        final int size = firstPoint.size;
        double[] centerCoordinates = new double[size];

        for (int i = 0; i < size; i++) {
            centerCoordinates[i] = firstPoint.coordinates[i]; // * firstPoint.weight;
        }
        int totalWeight = 1; //firstPoint.weight;

        if (centerCoordinates.length == 0){
            throw new IllegalArgumentException("Array has size 0");
        }

        // Sum of all points and weights
        while (iterator.hasNext()) {
            Point point = iterator.next();
            for (int i = 0; i < size; i++) {
                centerCoordinates[i] += point.coordinates[i]; // * point.weight;
            }
            //totalWeight += point.weight;
        }

        // Average using weight
        for (int i = 0; i < size; i++) {
            centerCoordinates[i] /= totalWeight;
        }

        return new Point(1, centerCoordinates);
    }

    public double[] getCoordinates() {
        return coordinates;
    }

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

    /*
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (double d : coordinates) {
            sb.append(d).append(",");
        }
        if(sb.length() == 0) {
            throw new IllegalArgumentException("The point has no coordinates");
        }
        sb.deleteCharAt(sb.length() - 1);
        return sb.toString();
    }

     */

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