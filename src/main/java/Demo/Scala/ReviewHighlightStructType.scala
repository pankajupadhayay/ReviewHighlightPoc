package com.basic.scala.StructType

import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession, types}

object StructTypeNestedJson extends App {

  val spark = SparkSession.builder().appName("SparkByExamples.com")
    .master("local[1]")
    .getOrCreate()

  val structureData = Seq(
    Row(Row(Row(Row(234, 345, Row("rating", "about", "reviewText", "author", "reviewText", "author", "snippetId", "summary", "submissionTime", "reviewTitle"))))),
    Row(Row(Row(Row(134, 145, Row("rating1", "about1", "reviewText1", "author1", "reviewText1", "author1", "snippetId1", "summary1", "submissionTime1", "reviewTitle1"))))),
    Row(Row(Row(Row(334, 445, Row("rating2", "about2", "reviewText2", "author2", "reviewText2", "author2", "snippetId2", "summary2", "submissionTime2", "reviewTitle2"))))))


  val structureData2 = Seq(
    Row(Row(Row(Row(234, 345, Row("rating", "about", "reviewText", "author", "reviewText", "author", "snippetId", "summary", "submissionTime", "reviewTitle")))
      , Row(Row(234, 345, Row("rating", "about", "reviewText", "author", "reviewText", "author", "snippetId", "summary", "submissionTime", "reviewTitle"))))))


  val positive_feature = "price"
  val negative_feature = "color"
  val structureSchema = new StructType()
    .add("subjects", new StructType()
      .add("positive", new StructType()
        .add(positive_feature, new StructType()
          .add("presenceCount", IntegerType)
          .add("mentionsCount", IntegerType)
          .add("bestExamples", new StructType()
            .add("rating", StringType)
            .add("about", StringType)
            .add("reviewText", StringType)
            .add("author", StringType)
            .add("snippetId", StringType)
            .add("summary", StringType)
            .add("submissionTime", StringType)
            .add("reviewTitle", StringType)
          )
        )
      )
      .add("negative", new StructType()
        .add(negative_feature, new StructType()
          .add("presenceCount", IntegerType)
          .add("mentionsCount", IntegerType)
          .add("bestExamples", new StructType()
            .add("rating", StringType)
            .add("about", StringType)
            .add("reviewText", StringType)
            .add("author", StringType)
            .add("snippetId", StringType)
            .add("summary", StringType)
            .add("submissionTime", StringType)
            .add("reviewTitle", StringType)
          )
        )
      )
    )

  val arrayDF = spark.createDataFrame(spark.sparkContext.parallelize(structureData2), structureSchema)
  arrayDF.printSchema()
  arrayDF.show()

  //Put that data to json format.
  println("**************" + structureSchema.prettyJson)

  //printing json
  arrayDF.write.mode("overwrite")
    .json("/Users/pankaj.upadhayay/Documents/Dev/SampleData/StructTypeBasic11.json")

}
