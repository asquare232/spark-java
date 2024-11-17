package com.asquare.chapter1;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

public class CreateRDDUsingParalleizeTest {

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

}
