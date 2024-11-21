package com.asquare.example.rdd;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.Scanner;
import java.util.stream.Collectors;
import java.util.stream.Stream;



public class SparkFirst {
    public static void main(String[] args) {
        System.out.println("Hello world!");

        try(var spark = SparkSession.builder().appName("SparkFirst")
                .master("local[*]")
                .getOrCreate();
            final var sc = new JavaSparkContext(spark.sparkContext());){

            final var list = Stream.iterate(1, n-> n+1).limit(5)
                    .collect(Collectors.toList());
            list.forEach(System.out::println);

            final var myRdd = sc.parallelize(list);
            System.out.printf("Total Element count in RDD::  %d%n", myRdd.count());
            System.out.printf("Total No of partition in RDD::  %d%n", myRdd.getNumPartitions());

            final var rddMin= myRdd.reduce(Integer::min);
            final var rddMax= myRdd.reduce(Integer::max);
            final var rddSum= myRdd.reduce(Integer::sum);

            System.out.printf("Min %d , Max %d , Sum %d ", rddMin, rddMax, rddSum);

            try (final var scanner = new Scanner(System.in);){
                scanner.nextLine();
            }
        }

    }
}