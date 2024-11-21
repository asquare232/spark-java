package com.asquare.example.datasets;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import scala.collection.immutable.Seq;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DatasetReduceTest {
    SparkSession sparkSession = SparkSession.builder().appName("DatasetReduceTest")
            .master("local[*]").getOrCreate();

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
    @DisplayName("Test reduce() method in Spark dataset")
    void testSparkReduceMethod(){
          var dataset = sparkSession.createDataset(data, Encoders.DOUBLE());
          dataset = dataset.repartition(14);

          final Instant start= Instant.now();

          for(int i =0; i < 10 ; i ++){
              final var result = dataset.agg(functions.sum("value")
                      .alias("total"));
              final var sum = result.collectAsList().get(0).getDouble(0);
              System.out.println("[Spark Dataset agg] SUM :" + sum);
          }
          final long timeElapsed = (Duration.between(start, Instant.now()).toMillis())/ 10;
          System.out.printf("[Spark Dataset agg: %d ms%n%n", timeElapsed);

    }

}

