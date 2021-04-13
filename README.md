# ReviewHighlightPoc

ReviewHighlightFeatureAPI_All is implemented  with scala case class to populate data.

ReviewHighlightStructType this have code where I am replacing scala case class by Spark StructType.



Spark SQL Queries

select sFeatureQuotes.product_key_feature_id,sFeatureQuotes.product_external_id,sFeatureQuotes.review_rating,sFeatureQuotes.review_legacy_id,sFeatureQuotes.en_translated_quote,sFeatureQuotes.review_date,sFeatureQuotes.native_language, sFeature.feature ,sFeatureQuotes.quote
FROM translatedProductSummaryKeyFeatures sFeature,translatedProductSummaryKeyFeatureQuotes sFeatureQuotes where sFeature.product_key_feature_id=sFeatureQuotes.product_key_feature_id and sFeature.product_external_id='005010115'

select product_key_feature_id,product_external_id,feature,feature_type,total_reviews_mentioned,(positive_reviews_mentioned+negative_reviews_mentioned) as mentionsCount  FROM translatedProductSummaryKeyFeatures  where product_external_id='005010115'


SQL queries

SELECT * FROM "bazaar_addstructure_scaffold"."translated_product_summary_key_feature_quotes"
where client='adidasglobal' and product_external_id='bem83' and native_language ='de' limit 10


select product_key_feature_id,product_external_id,feature,feature_type,total_reviews_mentioned,positive_reviews_mentioned,negative_reviews_mentioned
FROM translatedProductSummaryKeyFeatures where client='levis' and product_external_id='978110016'

SELECT product_key_feature_id,product_external_id,feature,feature_type,total_reviews_mentioned,positive_reviews_mentioned,negative_reviews_mentioned FROM "bazaar_addstructure_scaffold"."translated_product_summary_key_feature_quotes" where client='levis'
and product_external_id='978110016'
 limit 10

select product_key_feature_id,product_external_id,feature,feature_type,total_reviews_mentioned,positive_reviews_mentioned,negative_reviews_mentioned,(positive_reviews_mentioned+negative_reviews_mentioned) as mentionsCount
FROM "bazaar_addstructure_scaffold"."translated_product_summary_key_features" where client='levis' and product_external_id='978110016'
