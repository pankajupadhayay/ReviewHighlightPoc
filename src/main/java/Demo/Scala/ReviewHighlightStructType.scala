package com.basic.scala.StructType

import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession, types}

object ReviewHighlightStructType extends App {

  val spark = SparkSession.builder().appName("SparkByExamples.com")
    .master("local[1]")
    .getOrCreate()
  //This is sample data need to populate after retrieval from spark SQL
  val structureData = Seq(
    Row(Row(Map("fit" -> Row(234, 345, List(Row("rating", "about", "reviewText", "author", "reviewText", "author", "snippetId", "summary", "submissionTime", "reviewTitle")
      , Row("rating12", "about12", "reviewText12", "author34", "reviewText454", "author56", "snippetId56", "summary56", "submissionTime67", "reviewTitle65"))),
      "price" -> Row(34, 35, List(Row("rating", "about", "reviewText", "author", "reviewText", "author", "snippetId", "summary", "submissionTime", "reviewTitle")
        , Row("rating12", "about12", "reviewText124", "author341", "reviewText45", "author568", "snippetId568", "summary568", "submissionTime678", "reviewTitle658"))),
      "satisfaction" -> Row(24, 35, List(Row("rating", "about", "reviewText", "author", "reviewText", "author", "snippetId", "summary", "submissionTime", "reviewTitle")
        , Row("rating12", "about12", "reviewText125", "author342", "reviewText456", "author567", "snippetId567", "summary567", "submissionTime677", "reviewTitle657"))),
      "comford" -> Row(23, 34, List(Row("rating", "about", "reviewText", "author", "reviewText", "author", "snippetId", "summary", "submissionTime", "reviewTitle")
        , Row("rating12", "about12", "reviewText126", "author343", "reviewText457", "author564", "snippetId56", "summary564", "submissionTime67", "reviewTitle654")))
    ), Map("price" -> Row(234, 345, List(Row("rating", "about", "reviewText", "author", "reviewText", "author", "snippetId", "summary", "submissionTime", "reviewTitle")))))))

  val positive_feature = "Best"
  val negative_feature = "Worse"

  //This is Schema for Review Highlight display API
  val structureSchema = new StructType()
    .add("subjects", new StructType()
      .add(positive_feature, MapType(StringType, new StructType()
        .add("presenceCount", IntegerType)
        .add("mentionsCount", IntegerType)
        .add("bestExamples", ArrayType(new StructType()
          .add("rating", StringType)
          .add("about", StringType)
          .add("reviewText", StringType)
          .add("author", StringType)
          .add("snippetId", StringType)
          .add("summary", StringType)
          .add("submissionTime", StringType)
          .add("reviewTitle", StringType))
        )
      )
      )
      .add(negative_feature, MapType(StringType, new StructType()
        .add("presenceCount", IntegerType)
        .add("mentionsCount", IntegerType)
        .add("bestExamples", ArrayType(new StructType()
          .add("rating", StringType)
          .add("about", StringType)
          .add("reviewText", StringType)
          .add("author", StringType)
          .add("snippetId", StringType)
          .add("summary", StringType)
          .add("submissionTime", StringType)
          .add("reviewTitle", StringType))
        )
      )
      )
    )

  val arrayDF = spark.createDataFrame(spark.sparkContext.parallelize(structureData), structureSchema)
  arrayDF.printSchema()
  arrayDF.show()

  //Put that data to json format.
  println("**************" + structureSchema.prettyJson)

  //printing json
  arrayDF.write.mode("overwrite")
    .json("src/test/resources/output/ReviewHighlight.json")

}
