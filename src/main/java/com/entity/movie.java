package com.entity;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class movie implements Serializable {
    private Integer movieId;
    private String title;
    private String[] genres;
}
//    SparkSession spark = SparkSession.builder()
//            .master("local")
//            .appName("MongoSparkConnectorIntro")
//            .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/test.movies")
//            .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/test.movies")
//            .getOrCreate();
//
//    // Create a JavaSparkContext using the SparkSession's SparkContext object
//    JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
//
//    // Load data and infer schema, disregard toDF() name as it returns Dataset
//    Dataset<movie> DS=MongoSpark.load(jsc).toDS(movie.class);
//        DS.printSchema();
//                movie movie1=DS.first();
//                System.out.println(movie1.getMovieId());
//                System.out.println(movie1.getTitle());
//                System.out.println(movie1.getGenres());