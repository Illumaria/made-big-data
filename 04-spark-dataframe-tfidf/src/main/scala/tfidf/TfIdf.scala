package tfidf

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object TfIdf {
  def main(args: Array[String]): Unit = {
    // Create Spark session
    val spark: SparkSession = SparkSession.builder()
      // master address
      .master("local[*]")
      // app name in Spark interface
      .appName("tfidf")
      // get current or create new
      .getOrCreate()

    // syntactic sugar for convenient work with Spark
    import spark.implicits._

    // load dataset: https://www.kaggle.com/andrewmvd/trip-advisor-hotel-reviews
    val df: DataFrame = spark.read
      .option("header", "true")
      .option("inferScheme", "true")
      .csv("src/main/resources/tripadvisor_hotel_reviews.csv")
      // add document id column
      .withColumn("doc_id", monotonically_increasing_id())
      .cache()

    val processed: DataFrame = df
      // remove punctuation
      .withColumn("trimmed", trim(lower(regexp_replace($"Review", "[^\\sa-zA-Z0-9]", ""))))
      // split documents into single terms
      .withColumn("terms", split($"trimmed", " "))
      // find number of terms in each document
      .withColumn("num_terms_in_doc", size($"terms"))
      // final explode
      .select($"doc_id", explode($"terms").alias("term"), $"num_terms_in_doc")
      .cache()

    // count term frequency
    val tf: DataFrame = processed
      .groupBy($"doc_id", $"num_terms_in_doc", $"term")
      .count()
      .withColumn("tf", $"count" / $"num_terms_in_doc")

    // find total number of documents to use for idf
    val num_docs: Long = df.count()

    // count inverse document frequency
    val idf: DataFrame = processed
      // count documents with given term
      .groupBy($"term")
      .agg(countDistinct($"doc_id").alias("num_docs_with_term"))
      // get 100 most frequent terms
      .orderBy(desc("num_docs_with_term"))
      .limit(100)
      // calculate idf
      .withColumn("idf", log(lit(num_docs) / $"num_docs_with_term"))

    val tf_idf: DataFrame = idf
      // merge tf and idf dataframes by term values
      .join(tf, usingColumn = "term")
      // create new column with tf-idf value
      .withColumn("tf_idf", $"tf" * $"idf")
      // select only useful columns
      .select($"term", $"tf_idf", $"doc_id")

    /*
     * TF-IDF is usually represented in a [terms X documents] format.
     * Here, however, transposed approach is used since the number of documents
     * is significantly larger than the number of terms.
     * To get the classic view, swap the arguments (column names) in groupBy and pivot methods.
     */
    val pivoted: DataFrame = tf_idf
      .groupBy("doc_id")
      .pivot("term")
      .agg(expr("coalesce(first(tf_idf), 0)"))
      .na.fill(0.0)
      .orderBy(asc("doc_id"))

    pivoted.show(20, false)
  }
}