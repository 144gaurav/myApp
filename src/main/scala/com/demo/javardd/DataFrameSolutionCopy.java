package com.demo.javardd;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.functions;
import scala.Tuple3;
import org.apache.spark.api.java.JavaRDD;

import java.util.List;

import static org.apache.spark.sql.functions.*;


public class DataFrameSolutionCopy {

    public static void main(String[] args) throws Exception {

      //  SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("solution").set("spark.serializer","org.apache.spark.serializer.KryoSerializer");

        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .config("spark.master", "local[*]")
                .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        Dataset<Row> new_product_df = spark.read()
                .format("csv")
                .option("header", true)
                .option("inferSchema", true)
                .load("/home/gaurav/Downloads/extra/Product.csv");

        Dataset<Row> new_sale_df = spark.read().format("csv")
                .option("header", true)
                .option("inferSchema", true)
                .load("/home/gaurav/Downloads/extra/Sale.csv");

        Dataset<Row> yearWise_sale_df = new_sale_df
                .withColumn("year", year(new_sale_df.col("created_at")));

        Dataset<Row> grouped_sale_df = yearWise_sale_df
                .groupBy(yearWise_sale_df.col("year"), yearWise_sale_df.col("product_id"))
                .agg(sum("units").alias("units"));

        Dataset<Row> join_df = grouped_sale_df.join(new_product_df, grouped_sale_df.col("product_id")
                .equalTo(new_product_df.col("id")));
        Dataset<Row> total_units_df = join_df.selectExpr("year", "name", " units * unit_price as total");

        WindowSpec windowSpec = Window.partitionBy(total_units_df.col("year"))
                .orderBy(total_units_df.col("total").desc());

        Dataset<Row> window_join_df = total_units_df.withColumn("rank", rank().over(windowSpec));

        Dataset<Row> rank_filter_df = window_join_df.where(window_join_df.col("rank").equalTo(1));

        Dataset<Row> final_df = rank_filter_df.groupBy("year", "total")
                .agg(functions.concat_ws(",", functions.sort_array(functions.collect_list(rank_filter_df.col("name")))).alias("top"))
                .orderBy(rank_filter_df.col("year"));
       JavaRDD<Tuple3< ?, ?, ? >> output = final_df.toJavaRDD().map(x->{
              return new Tuple3(x.get(0),x.get(2), x.get(1));
       });
        List < Tuple3 < ?,?, ? >> output1 = output.collect();
        System.out.println("year," + "top," + "total");
        String top = "";
        for (Tuple3 < ? ,?,? > tuple : output1) {
            top = tuple._2().toString();
            if(tuple._2().toString().split(",").length>1)
                 top = "\""+tuple._2()+"\"";
            System.out.println(tuple._1() + "," + top +","+tuple._3() );
        }

    }
}
