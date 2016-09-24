/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
package org.strata.workshop

// $example on$

import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, RegressionEvaluator}
import org.apache.spark.ml.feature.{VectorAssembler, StringIndexer}
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.types._

import org.apache.spark.sql.{SQLContext, _}

import org.apache.spark.ml.feature.StandardScaler

// $example off$
import org.apache.spark.sql.SparkSession

// https://forge.scilab.org/index.php/p/rdataset/source/tree/master/csv/datasets/mtcars.csv
object KMeans {

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("KMeans")
      .master("local")
      .getOrCreate()
    import spark.implicits._

    val customSchema = StructType(Array(
      StructField("symboling", StringType, true),
      StructField("normalized-losses", StringType, true),
      StructField("make", StringType, true),

      StructField("fuel-type", StringType, true),
      StructField("aspiration", StringType, true),

      StructField("num-of-doors", StringType, true),
      StructField("body-style", StringType, true),

      StructField("drive-wheels", StringType, true),
      StructField("engine-location", StringType, true),

      StructField("wheel-base", StringType, true),
      StructField("length", StringType, true),
      StructField("width", StringType, true),
      StructField("height", StringType, true),
      StructField("curb-weight", StringType, true),

      StructField("engine-type", StringType, true),
      StructField("num-of-cylinders", StringType, true),
      StructField("engine-size", StringType, true),
      StructField("fuel-system", StringType, true),
      StructField("bore", StringType, true),
      StructField("stroke", StringType, true),

      StructField("compression-ratio", StringType, true),

      StructField("horsepower", StringType, true),
      StructField("peak-rpm", StringType, true),
      StructField("city-mpg", IntegerType, true),
      StructField("highway-mpg", IntegerType, true),
      StructField("price", IntegerType, true)

    ))

    val ds = spark.read.option("inferSchema", "true").option("header", "true").option("nullValue", "?").csv("data/mtcars.csv")

    ds.printSchema()
    ds.show()

    // vector assembler
    val assembler = new VectorAssembler()
      .setInputCols(Array("mpg", "cyl", "disp", "hp", "drat", "wt"))
      .setOutputCol("features")
      
      val assemdata = assembler.transform(ds)
       
     val scaled = new StandardScaler()
       .setInputCol("features")
       .setOutputCol("scaledFeatures")
       .setWithStd(true)
       .setWithMean(true)
 
     // Compute summary statistics by fitting the StandardScaler.
     val scalerModel = scaled.fit(assemdata)
 
     // Normalize each feature to have unit standard deviation.
     val scaledData = scalerModel.transform(assemdata)
  
     val clusters = 10
      // Trains a k-means model
     val kmeans = new KMeans()
       .setK(clusters)
       .setMaxIter(1000)
       .setFeaturesCol("scaledFeatures")
       .setPredictionCol("prediction")
       
    val model = kmeans.fit(scaledData)
  

    // Evaluate clustering by computing Within Set Sum of Squared Errors.
    val WSSSE = model.computeCost(assemdata)
    println(s"Within Set Sum of Squared Errors = $WSSSE")

    // Shows the result.
    println("Cluster Centers: ")
    model.clusterCenters.foreach(println)

    // predict
    val predict = model.transform(assemdata)
    predict.show(1000)
    
    for (i <- 0 to clusters) { 
      predict.filter(col("prediction") === i)
       .select(col("_c0"), col("features"), col("prediction"))
       .collect
       .foreach(println)
    }

    spark.stop()
  }
}
// scalastyle:on println


