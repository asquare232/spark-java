package com.asquare.example.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CreateRDDUsingParallalizeTest {

    private final SparkConf sc = new SparkConf().setAppName("CreateRDDUsingParalleizeTest")
                                    .setMaster("local[*]");

    @Test
    @DisplayName("Create Empty RDD with no partition ")
    public void testWithNoPartition(){
        try(final var sparkContext = new JavaSparkContext(sc);){
        final var emptyRDD = sparkContext.emptyRDD();
        System.out.println(emptyRDD);
        System.out.printf("No. of partition %d%n", emptyRDD.getNumPartitions());

        }
    }

    @Test
    @DisplayName("Create Empty RDD with default partition ")
    public void testWithDefaultPartition(){
        try(final var sparkContext = new JavaSparkContext(sc);){
            final var emptyRDD = sparkContext.parallelize(List.of());
            System.out.println(emptyRDD);
            System.out.printf("No. of partition %d%n", emptyRDD.getNumPartitions());

        }
    }


    @Test
    @DisplayName("Create  RDD with parallelize count ")
    public void createSparkRDDwithParallelizeCount(){
        try(final var sparkContext = new JavaSparkContext(sc);){
            var data = Stream.iterate(1, n-> n+1)
                    .limit(8)
                    .collect(Collectors.toList());
            final var dataRDD = sparkContext.parallelize(data, 8);
            System.out.println(dataRDD);
            System.out.printf("No. of partition %d%n", dataRDD.getNumPartitions());
            dataRDD.collect().forEach(System.out::println);

        }
    }

}
