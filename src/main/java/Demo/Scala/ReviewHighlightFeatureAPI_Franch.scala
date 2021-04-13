package Demo.Scala

import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer


object ReviewHighlightFeatureAPI_Franch extends App {

  println("##################Spark Object creation#############################")
  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExamples.com")
    .getOrCreate()

  // Replace Key with your AWS account key (You can find this on IAM
  println("################## AWS S3 connection #############################")
  spark.sparkContext
    .hadoopConfiguration.set("fs.s3a.access.key", "***********")
  // Replace Key with your AWS secret key (You can find this on IAM service)
  spark.sparkContext
    .hadoopConfiguration.set("fs.s3a.secret.key", "****")
  spark.sparkContext
    .hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")

  val startTimeMillis = System.currentTimeMillis()
  println("###############Spark load translatedProductSummaryKeyFeatures parquet format##############################")
  val parquetFileDF_feature = spark.read.parquet("s3a://addstructure-review-highlights-prod/data/product-summaries/key-features/prod/bazaar/release/contents/full/v1/client=levis/part-r-0-data.parquet")
  println("################ Display table data ###################################")
  // Parquet files can also be used to create a temporary view and then used in SQL statements
  parquetFileDF_feature.createOrReplaceTempView("translatedProductSummaryKeyFeatures")
  val namesDF_feature = spark.sql("select product_key_feature_id,product_external_id,feature,feature_type,total_reviews_mentioned,(positive_reviews_mentioned+negative_reviews_mentioned) as mentionsCount  FROM translatedProductSummaryKeyFeatures where product_external_id='978110016'")
  namesDF_feature.show()
  val collected = namesDF_feature.collect()


  println("###############Scala case classes to created nested JSON Output ##############################")
  case class ProductSummaryFeatures(var product_key_feature_id: String, var product_external_id: String, var feature: String, var feature_type: String, var presenceCount: Int,
                                    var mentionsCount: Int, var BestExamples:Array[FeatureQuote])
  case class Root (subjects: Structure)
  case class Structure(positive: ProductSummaryFeaturesList, negative: ProductSummaryWorseFeaturesList)
  case class ProductFeatures(var product_key_feature_id: String, var product_external_id: String, var feature: String, var feature_type: String, var presenceCount: Int,
                             var positive_reviews_mentioned: Int, var negative_reviews_mentioned: Int)


  println("##################Spark Read Data from translatedProductSummaryKeyFeatureQuotes parquet format##################")
  val parquetFileDF_quotes = spark.read.parquet("s3a://addstructure-review-highlights-prod/data/product-summaries/key-feature-quotes/prod/bazaar/release/contents/full/v1/client=levis/part-r-0-data.parquet")
  println("################ Display table data #######################")
  // Parquet files can also be used to create a temporary view and then used in SQL statements
  parquetFileDF_quotes.createOrReplaceTempView("translatedProductSummaryKeyFeatureQuotes")
  val namesDF_quotes = spark.sql("select sFeatureQuotes.product_key_feature_id,sFeatureQuotes.product_external_id,sFeatureQuotes.review_rating,sFeatureQuotes.review_legacy_id,sFeatureQuotes.en_translated_quote,sFeatureQuotes.review_date,sFeatureQuotes.native_language, sFeature.feature, sFeatureQuotes.quote FROM translatedProductSummaryKeyFeatures sFeature,translatedProductSummaryKeyFeatureQuotes sFeatureQuotes where sFeature.product_key_feature_id=sFeatureQuotes.product_key_feature_id and sFeature.product_external_id='978110016'")
  namesDF_quotes.show()
  val featureQuoteCollected = namesDF_quotes.collect()

  //Load time calculation
  val endTimeMillisexecuting = System.currentTimeMillis()
  val durationSecondsExecute = (endTimeMillisexecuting - startTimeMillis) / 1000
  println("Time to execute both parquet data set  in seconds >>>>>>>>>>"+durationSecondsExecute)


  case class FeatureQuote(var product_key_feature_id: String, var product_external_id: String, var review_rating: Int, var about: String, var reviewsText: String,
                          var author: String, var snippetId: String, var reviewId: String, var summary_en: String, submissionTime: String, reviewTitle: String, var summary: String, var native_language: String)


  var featureQuoteList = new ListBuffer[FeatureQuote]()
  var featureQuoteWorstList = new ListBuffer[FeatureQuote]()
  var aMapFeatureQuote: Map[String, ListBuffer[FeatureQuote]] = Map()


  featureQuoteCollected.foreach(row => {
    val product_key_feature_id = row.getString(0)
    val product_external_id = row.getString(1)
    val review_rating = row.getInt(2)
    val review_legacy_id = row.getString(3)
    val en_translated_quote = row.getString(4)
    val review_date = row.getString(5)
    val native_language = row.getString(6)
    val feature=row.getString(7)
    val quote=row.getString(8)

    if (aMapFeatureQuote.contains(product_key_feature_id)) {
      featureQuoteList = aMapFeatureQuote.get(product_key_feature_id).get
      featureQuoteList += (FeatureQuote(product_key_feature_id, product_external_id, review_rating, feature, "reviewsText", "author",
        review_legacy_id, review_legacy_id, en_translated_quote, review_date, "Title",quote,native_language))
    }
    else {
      featureQuoteList = new ListBuffer[FeatureQuote]()
      featureQuoteList += (FeatureQuote(product_key_feature_id, product_external_id, review_rating, feature, "reviewsText", "author",
        review_legacy_id, review_legacy_id, en_translated_quote, review_date, "Title",quote,native_language))
      aMapFeatureQuote += (product_key_feature_id -> featureQuoteList)
    }
  })
  println("###############Scala load feature parquet data to Scala Object ##############################")
  var aMapFeatureBest: Map[String, ProductSummaryFeatures] = Map()
  var aMapFeatureWorse: Map[String, ProductSummaryFeatures] = Map()


  var arrayFeatureBest = new ListBuffer[String]()
  var arrayFeatureWorse = new ListBuffer[String]()

  collected.foreach(row => {
    val product_key_feature_id = row.getString(0)
    val product_external_id = row.getString(1)
    var feature = row.getString(2)
    val feature_type = row.getString(3)
    val presenceCount = row.getInt(4)
    val mentionsCount = row.getInt(5)
    var featureQuoteListByProductFId=aMapFeatureQuote.get(product_key_feature_id).get.toArray
    if (feature_type.equalsIgnoreCase("Best")) {
      val productfetureB=ProductSummaryFeatures(product_key_feature_id, product_external_id, feature, feature_type,
        presenceCount, mentionsCount,featureQuoteListByProductFId)
      aMapFeatureBest += (feature -> productfetureB)
      arrayFeatureBest+=(feature)
    }
    else {
      val productfetureW=ProductSummaryFeatures(product_key_feature_id, product_external_id, feature, feature_type,
        presenceCount, mentionsCount,featureQuoteListByProductFId)
      aMapFeatureWorse += (feature -> productfetureW)
      arrayFeatureWorse+=(feature)
    }
  })
  var arrayArrayFeatureBest = arrayFeatureBest.toArray
  var arrayArrayFeatureWorse = arrayFeatureWorse.toArray
  case class ProductSummaryFeaturesList(satisfaction:ProductSummaryFeatures,quality:ProductSummaryFeatures,comfort:ProductSummaryFeatures,color:ProductSummaryFeatures)
  val productFeatureBest=ProductSummaryFeaturesList(aMapFeatureBest.get(arrayArrayFeatureBest.lift(0).get).get,aMapFeatureBest.get(arrayArrayFeatureBest.lift(1).get).get,aMapFeatureBest.get(arrayArrayFeatureBest.lift(2).get).get,aMapFeatureBest.get(arrayArrayFeatureBest.lift(3).get).get)

  case class ProductSummaryWorseFeaturesList(large:ProductSummaryFeatures,disappointing:ProductSummaryFeatures,wearandtear:ProductSummaryFeatures,thickness:ProductSummaryFeatures)
  val productFeatureWorse=ProductSummaryWorseFeaturesList(null,null,null,null)

  var structure = Structure(productFeatureBest: ProductSummaryFeaturesList, productFeatureWorse: ProductSummaryWorseFeaturesList)
  var root= Root (structure: Structure)
  println("#####################create a JSON string from the RH, then print it#########################")
  import com.google.gson.GsonBuilder
  val gson = new GsonBuilder().setPrettyPrinting.create()
  val json = gson.toJson(root)
  println(json)

  //TODO push the change to s3
  // PrintWriter
  import java.io._
  val pw = new PrintWriter(new File("/Users/pankaj.upadhayay/Documents/Dev/SampleData/ReviewHighlight_Franch.json" ))
  pw.write(json)
  pw.close



}
