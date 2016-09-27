
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

package org.strata.workshop

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

import org.apache.spark.ml.feature.{CountVectorizer, NGram, RegexTokenizer, StopWordsRemover}
import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.feature.Stemmer
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql._
import org.apache.spark.sql.types.LongType
import org.apache.spark.{SparkConf, SparkContext}
import com.databricks.spark.avro._
import org.apache.spark.sql.functions._
import org.apache.log4j.Logger
import org.apache.log4j.Level



/**
  * Infer the cluster topics on a set of 20 newsgroup data.
  *
  * The input text is text files, corresponding to emails in the newsgroup.
  * Each text file corresponds to one document.
  *
  *
  */
object TopicModelingWithStemmer {
  val a1 = udf ({ text: String =>

   /** text.replace("UNCLASSIFIED U.S. Department of State Case No.","A")
      .replace("F-2015-04841", "A")
      .replace("Doc No.", "A")
      .replaceAll("[^A-Za-z ]", " ")
  }
  )*/

	def main (args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    
   var inputDir = "data/topicmodeling/newsgroup_20/"
    var stopWordFile = "data/topicmodeling/stopwords.txt"

    if(args.length > 1) {
      inputDir = args(0)
      stopWordFile = args(1)
    }

    val sparkConf = new SparkConf().setAppName("TopicModelingWithStemmer")
        sparkConf.setMaster("local[8]")
                .set("spark.broadcast.compress", "false")
                .set("spark.shuffle.compress", "false")
                .set("spark.shuffle.spill.compress", "false")
                 .set("spark.io.compression.codec", "lzf")
    val sc = new SparkContext(sparkConf)

    val numTopics: Int = args(2).toInt
    val numNgrams: Int = args(3).toInt

    val maxIterations: Int = 1000
    val vocabSize: Int = 2000

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    import sqlContext.implicits._


    val rawEmailDF = sqlContext.read.avro(inputDir)
    val DF = rawEmailDF.rdd.map(_.getString(0)).zipWithIndex().toDF("text","docId")
    //val test = docDF.take(10)
    val docDF = DF.withColumn("text", a1(DF.col("text"))).cache


    val stemmed = new Stemmer()
                    .setInputCol("text")
                    .setOutputCol("stemmed")
                    .setLanguage("English")
                    .transform(docDF)
    //stemmed.show()
    val tokens = new RegexTokenizer()
                    .setGaps(false)
                    .setPattern("\\w+")
                    .setMinTokenLength(4)
                    .setInputCol("stemmed")
                    .setOutputCol("words")
                    .transform(stemmed)
    //tokens.show()
    val stopwords = sc.textFile(stopWordFile).collect
    val filteredTokens = new StopWordsRemover()
                          .setStopWords(stopwords)
                          .setCaseSensitive(false)
                          .setInputCol("words")
                          .setOutputCol("filtered")
                          .transform(tokens)
    //filteredTokens.show()


    val ngram = new NGram().setN(numNgrams)
                  .setInputCol("filtered")
                  .setOutputCol("ngrams")
                  .transform(filteredTokens)

    /**val hashingTF = new HashingTF()
                      .setInputCol("ngrams")
                      .setOutputCol("rawFeatures")
                      .setNumFeatures(100)
                      .transform(ngram)
    val idf = new IDF()
                .setInputCol("rawFeatures")
                .setOutputCol("features")
                .fit(hashingTF)**/
                //.transform(hashingTF)

    val cvModel = new CountVectorizer()
       .setInputCol("ngrams")//filtered")
       .setOutputCol("features")
       .setMinDF(10)
       .setMinTF(0.05)
       .setVocabSize(vocabSize)
       .fit(ngram)

    val countVectors = cvModel.transform(ngram)
                        .select("docId", "features")
                        .map {
                            case Row(docId: Long, countVector: Vector)
                            => (docId, countVector)
                        }.cache()

    /**val countVectors = cvModel
      * .transform(ngram)//filteredTokens)
      * .select("docId", "features")
      * .map {
      * case Row(docId: Long, countVector: Vector) => (docId, countVector)
      * }.cache()**/

    val mbf = {
      val corpusSize = countVectors.count()
      2.0 / maxIterations + 1.0 / corpusSize
    }

    val lda = new LDA()
                  .setOptimizer(new EMLDAOptimizer())
                  .setK(numTopics)
                  .setMaxIterations(100)
                  .setDocConcentration(-1)
                  .setTopicConcentration(-1)

    val startTime = System.nanoTime()
    val ldaModel = {
      lda.run(countVectors)
    }

    val elapsed = (System.nanoTime() - startTime) / 1e9

    println(s"Finished training LDA model.  Summary:")
    println(s"Training time (sec)\t$elapsed")
    println(s"==========")

    val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = 10)
    val vocabArray = cvModel.vocabulary
    val topics = topicIndices.map {
                  case (terms, termWeights) =>
                  terms.map(vocabArray(_)).zip(termWeights)
    }

    topics.zipWithIndex.foreach {
          case (topic, i) => println(s"TOPIC $i")
          topic.foreach { case (term, weight) => println(s"$term\t$weight") }
          println(s"==========")
    }

    var distLDAModel: org.apache.spark.mllib.clustering.DistributedLDAModel = null
    if (ldaModel.isInstanceOf[DistributedLDAModel]) {
      distLDAModel = ldaModel.asInstanceOf[DistributedLDAModel]
    }
    /**val docsperDoc = distLDAModel.topDocumentsPerTopic(5)
    docsperDoc
      .foreach{
        case (docs, wts) => println("===========")
        for(i <- 1 to 5) {
          println( docs(i-1) )
          val a = docDF.filter( docDF("docId") === docs(i - 1) )
          println( a.select(a("text"))
                    .collect
                    .foreach( println )
          )
        }
      }**/
  }
}
