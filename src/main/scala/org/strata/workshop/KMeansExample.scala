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

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.{StandardScaler, VectorAssembler}
import org.apache.spark.sql.functions._

// $example off$
import org.apache.spark.sql.SparkSession

// https://forge.scilab.org/index.php/p/rdataset/source/tree/master/csv/datasets/mtcars.csv
object KMeansExample {

    def main(args: Array[String]) {

        Logger.getLogger("org").setLevel(Level.OFF)
        Logger.getLogger("akka").setLevel(Level.OFF)

        val spark = SparkSession
            .builder
            .appName("KMeansExample")
            .master("local")
            .getOrCreate()

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
        val WSSSE = model.computeCost(scaledData)
        println(s"Within Set Sum of Squared Errors = $WSSSE")

        // Shows the result.
        println("Cluster Centers: ")
        model.clusterCenters.foreach(println)

        // predict
        val predict = model.transform(scaledData)
        predict.show(1000)

        for (i <- 0 to clusters) {
            val predictionsPerCol = predict.filter(col("prediction") === i)
            println(s"Cluster $i")
            predictionsPerCol
                .select(col("_c0"), col("features"), col("prediction"))
                .collect
                .foreach(println)
            println("======================================================")
        }

        spark.stop()
    }
}

// scalastyle:on println


