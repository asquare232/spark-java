package com.asquare.example.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.stream.Stream;

public class CreateRDDExternalTest {

    private final SparkConf sc = new SparkConf().setAppName("CreateRDDUsingParalleizeTest")
            .setMaster("local[*]");


    @ParameterizedTest
    @ValueSource(strings = {
            "src\\test\\resources\\1000words.txt",
            "src\\test\\resources\\wordslist.txt.gz"
    })
    @DisplayName("Test loading local file into Spark RDD")
    public void testFileintoSParkRDD(String location){
        try(final var sparkContext = new JavaSparkContext(sc);){
            final var myRDD = sparkContext.textFile(location);
            System.out.printf("Total lines in the file : %d%n", myRDD.count());
            System.out.printf("Printing 1st 10 lines");
            myRDD.take(10).forEach(System.out::println);
        }
    }


    @ParameterizedTest
    @MethodSource("getFilePath")
    @DisplayName("Test loading local file into Spark RDD by method arg")
    public void testFileintoSparkRDDFromMethodSource(String location){
        try(final var sparkContext = new JavaSparkContext(sc);){
            final var myRDD = sparkContext.textFile(location);
            System.out.printf("Total lines in the file : %d%n", myRDD.count());
            System.out.printf("Printing 1st 10 lines");
            myRDD.take(10).forEach(System.out::println);
        }
    }

    @Test
    @DisplayName("Test loading whole directory file into Spark RDD")
    public void testWholeDirectoryIntoSparkRDD(){
        try(final var sparkContext = new JavaSparkContext(sc);){
            String tempDirPath = Path.of("src","test","resources").toString();
            final var myRDD = sparkContext.wholeTextFiles(tempDirPath);
            System.out.printf("Total no. of files in directory: %s = %d%n", tempDirPath, myRDD.count());
            myRDD.collect().forEach(tuple -> {
                System.out.printf("File name : %s%n", tuple._1);
                System.out.println("-------------------------------");
                if(tuple._1.endsWith("properties")){
                    System.out.printf("Content of [%s]:%n", tuple._1);
                    System.out.println(tuple._2);
                }
            });

        }
    }

    @Test
    @DisplayName("Test loading whole directory file into Spark RDD")
    public void testLoadingCSVFileIntoSparkRDD(){
        try(final var sparkContext = new JavaSparkContext(sc);){
            String csvFilePath = Path.of("src","test","resources","dma.csv").toString();
            final var myRDD = sparkContext.textFile(csvFilePath);
            System.out.printf("Total no. of files in directory: %s = %d%n", csvFilePath, myRDD.count());
            System.out.println("CSV Headers: ");
            System.out.println(myRDD.first());

            final var csvFields = myRDD.map(line -> line.split(","));
            csvFields.take(5)
                    .forEach(fields -> System.out.println(String.join("|", fields)));

        }
    }

    private static Stream<Arguments> getFilePath(){
        return Stream.of(
                Arguments.of(Path.of("src","test","resources", "1000words.txt").toString()),
                Arguments.of(Path.of("src","test","resources", "wordslist.txt.gz").toString())
        );
    }
}
