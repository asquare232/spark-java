package com.asquare.example.rdd.reduce;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RDDReduceTest {

    private final SparkConf sc = new SparkConf().setAppName("RDDReduceTest")
            .setMaster("local[*]");

    private static final List<Double> data = new ArrayList<>();

    @BeforeAll
    static void beforeAll(){
        final var dataSize = 1_000_000;
        for(int i =0; i<dataSize; i++){
            data.add(100* ThreadLocalRandom.current().nextDouble() * 47);
        }
        assertEquals(dataSize, data.size());
    }

    @Test
    @DisplayName("Test reduce() method in Spark RDD")
    void testSparkReduceMethod(){
      try(final var sparkContext = new JavaSparkContext(sc)){
          final var myRdd = sparkContext.parallelize(data, 14);
          final Instant start= Instant.now();

          for(int i =0; i < 10 ; i ++){
              final var sum = myRdd.reduce(Double::sum);
              System.out.println("[SPark RDD Reduce] SUM :" + sum);
          }
          final long timeElapsed = (Duration.between(start, Instant.now()).toMillis())/ 10;
          System.out.printf("[Spark RDD Reduce: %d ms%n%n", timeElapsed);

      }
    }

}

