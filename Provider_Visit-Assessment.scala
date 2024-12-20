
// Question 1:Given the two data datasets, calculate the total number of visits per provider. The resulting set should contain the provider's ID, name, specialty, along with the number of visits. Output the report in json, partitioned by the provider's specialty.


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


	// Define the explicit schema for providers.csv
	val providersSchema = StructType(Array(
	StructField("provider_id", IntegerType, nullable = false),
	StructField("provider_specialty", StringType, nullable = true),
	StructField("first_name", StringType, nullable = true),
	StructField("middle_name", StringType, nullable = true),
	StructField("last_name", StringType, nullable = true)
	))

	// Define schema for visits
	val visitsSchema = StructType(Array(
	StructField("visit_id", IntegerType, nullable = false),
	StructField("provider_id", IntegerType, nullable = false),
	StructField("date_of_service", DateType, nullable = false)
	))

	// Read the CSV files
    val providersDF = spark.read.format("csv")
      .option("header", "true")
      .schema(providersSchema)
      .option("delimiter","|")
      .load("/data/providers.csv")

 

    val visitsDF = spark.read.format("csv")
      .option("header", "true")
      .schema(visitsSchema)
      .option("delimiter",",")
      .load("/data/visits.csv")



    // Join the two DataFrames
    val joinedDF = providersDF.join(visitsDF, "provider_id")


    // Group by provider details and count visits
    val resultDF = joinedDF
      .groupBy("provider_id", "first_name", "middle_name", "last_name", "provider_specialty")
      .agg(count("visit_id").as("total_visits"))

    // Write the result in JSON format, partitioned by specialty
    resultDF.write.format("json")
      .partitionBy("provider_specialty")
      .save("/data/output/question1")



// Question2: Given the two datasets, calculate the total number of visits per provider per month. The resulting set should contain the provider's ID, the month, and total number of visits. Output the result set in json.


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

   
	// Define the explicit schema for providers.csv
	val providersSchema = StructType(Array(
	StructField("provider_id", IntegerType, nullable = false),
	StructField("provider_specialty", StringType, nullable = true),
	StructField("first_name", StringType, nullable = true),
	StructField("middle_name", StringType, nullable = true),
	StructField("last_name", StringType, nullable = true)
	))

	// Define schema for visits
	val visitsSchema = StructType(Array(
	StructField("visit_id", IntegerType, nullable = false),
	StructField("provider_id", IntegerType, nullable = false),
	StructField("date_of_service", DateType, nullable = false)
	))

	// Read the CSV files
    val providersDF = spark.read.format("csv")
      .option("header", "true")
      .schema(providersSchema)
      .option("delimiter","|")
      .load("/data/providers.csv")

 

    val visitsDF = spark.read.format("csv")
      .option("header", "true")
      .schema(visitsSchema)
      .option("delimiter",",")
      .load("/data/visits.csv")



    // Extract the month from the date_of_service column
    val visitsWithMonthDF = visitsDF
      .withColumn("month", date_format(col("date_of_service"), "yyyy-MM"))

    // Group by provider_id and month to calculate total visits per month
    val visitsByMonthDF = visitsWithMonthDF
      .groupBy("provider_id", "month")
      .agg(count("visit_id").alias("total_visits"))


    // Write the result to JSON format
    visitsByMonthDF.write.format("json")
      .mode("overwrite")
      .save("/data/output/question2")

