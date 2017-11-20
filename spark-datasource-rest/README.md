# Rest Data Source for Apache Spark

- This is a library for getting data from Rest based micro services/APIs for multiple input data points (in parallel) and collating the results, retuned by the API calls, in a Dataframe of Rows of Structure.

- Rest based micro services (for example Address Validation services, Google Search API, Watson Natural Language Proceing API, etc.) typically return result as one single record at a time for a specific value of imput parameters. However, for many Data Science problems same API needs to be called for multiple times for thousands of input Data points in hand (e.g. validating Address for a set of target customers, Getting personality insights from thousands of Tweets, getting Provider's details from NPI registry for a list of potential Doctors, etc.)

- This package supports calling the target micro service API in a distributed way for different data points. Also returns the results in a Structure (in a Dataframe) specific to the API signature without user having to specify the same.

## Requirements

This library requires Spark 2.0+ .

## Dependency

This library depends on [scalaj_http](https://github.com/scalaj/scalaj-http) package

## Using with Spark shell
This package can be added to  Spark using the `--packages` of `--jars` command line option.  For example, to include it when starting the spark shell as jar from the same folder use :

```
$SPARK_HOME/bin/spark-shell --jars spark-datasource-rest_2.11-2.1.0-SNAPSHOT.jar --packages org.scalaj:scalaj-http_2.10:2.3.0

```

## Features
This package allows multiple calls in parallel to a target Rest based Microservice for a set of different input data points. The input data points can be passed as a Temporary Spark Table. The column names of the table should be same as the keys of the target API. The rows in the table will have the values for the keys for which the target API has to be called multiple times (one time for one combination of the values of the keys). The result from the multiple calls of the API is returned as a [Spark DataFrames](https://spark.apache.org/docs/1.6.0/sql-programming-guide.html) of Rows of the resultant structure returned by the target API. 

For calling the target Rest service this library supports several options:
* `url`: This is the uri of the target Microservice. You can also provide the common parameters (those don;t vary with the input data) in this url. This is a mandatory parameter.
* `input`: You need to pass the name of the Temporary Spark Table which contains the input data set. This is a mandatory parameter too.
* `method`: The supported http/https method. Possible types supported right now are `POST`, and `GET`. Default is `POST`
* `userId` : The userId in case the target API needs basic authentication.
* `userPassword` : The password in case the target API needs basic authentication
* `partitions`: Number of partition to be used to increase parallelism. Default is 2.
* `connectionTimeout` : In case the target API needs high time to connect. Default is 1000 (in ms)
* `readTimeout` : In case the target API returns large volume of data which needs more read time. Default is 5000 (in ms)
* `schemaSamplePcnt` : Percentage of number of records in the input table to be used to infer teh schema. The default is "30" and minimum is 3. Incarese this number in case you are getting error or the schema is not propery inferred.
* `callStrictlyOnce` : This value is used to ensure if the backend API would be called only once for each input value or not. The default is "N". In that case the back end API may get called for multiple times - once for inferring the schema and then for other operations. If this value is set to "Y" the backend API would be called only once (during infering the schema) for all of the input data points and would be cached. This option is useful when the target APIs are paid service or does not support calls per day/per hour beyond certain number. However, the results would be cached which will increase the memory usage.

## Typical Structure of the Dataframe returned by Rest Data Source

The dataframe created by this Rest Data Source will return a set of Rows of same Structure. The Structure internally will contain the input fields with the names same as those passed through the input table. The structure will also contain the output returned by the target Rest API under the field 'output'. Whatever gets returned within 'output' field would be specific to the Rest API being called. But the structure of the same can be easily obtained by printSchema method of Dataframe.

Here below goes an example of the structure retutned by Rest Data Source when the target Rest API was [Watson API for Natural Language Understanding (with sentiment as feature)] (https://www.ibm.com/watson/developercloud/natural-language-understanding/api/v1/).

```scala

root
 |-- output: struct (nullable = true)
 |    |-- code: long (nullable = true)
 |    |-- error: string (nullable = true)
 |    |-- language: string (nullable = true)
 |    |-- retrieved_url: string (nullable = true)
 |    |-- sentiment: struct (nullable = true)
 |    |    |-- document: struct (nullable = true)
 |    |    |    |-- label: string (nullable = true)
 |    |    |    |-- score: double (nullable = true)
 |    |-- usage: struct (nullable = true)
 |    |    |-- features: long (nullable = true)
 |    |    |-- text_characters: long (nullable = true)
 |    |    |-- text_units: long (nullable = true)
 |-- q: string (nullable = true)
 |-- url: string (nullable = true)
 
 ```
 
In this example 'q' and 'url' are the input parameters passed for each data point. So the Temporary Spark Table, that used as input table, had two columns - 'q' and 'url'. Each row of that table had different values for 'q' and 'url'. The 'output' field contains the result returned by the Wtson API for natural language understanding. 

Sometimes there could be an additional field under root, namely '_corrupt_records'. This field will contain the outputs for the records for which the API returned an error.


## Examples

The examples below shows how to use this Rest Data Source for SODA api. The examples here get the Socrata dataset using SODA API. The columns in the Socrata dataset is presented by a field in the SODA API. Records are searched for using filters and SoQL queries (https://dev.socrata.com/docs/queries/). We will be using the filters with our API call.


### Scala API

```scala
// Create the target url string for Soda API for Socrata data source
val sodauri = "https://soda.demo.socrata.com/resource/6yvf-kk3n.json"

//Say we need to call the API for 3 sets of input data points for different values of 'region' and 'source'. The 'region' and 'source' are two filters supported by the SODA API for Socrata data source

val sodainput1 = ("Nevada", "nn")
val sodainput2 = ("Northern California", "pr")
val sodainput3 = ("Virgin Islands region", "pr")

// Now we create a RDD using these input data points

val sodainputRdd = sc.parallelize(Seq(sodainput1, sodainput2, sodainput3))

// Now we need to create the dataframe specifying column name for tghe dataframe same as the filter names
val sodainputKey1 = "region"
val sodainputKey2 = "source"

val sodaDf = sodainputRdd.toDF(sodainputKey1, sodainputKey2)

// And we create a temporary table now using the sodaDf
sodaDf.createOrReplaceTempView("sodainputtbl")

// Now we create the parameter map to pass to teh Rest Data Source.

val parmg = Map("url" -> sodauri, "input" -> "sodainputtbl", "method" -> "GET", "readTimeout" -> "10000", "connectionTimeout" -> "2000", "partitions" -> "10")

// Now we create the Dataframe which contains the result from the call to the Soda API for the 3 different input data points
val sodasDf = spark.read.format("org.apache.dsext.spark.datasource.rest.RestDataSource").options(parmg).load()

// We inspect the structure of the results returned. For Soda data source it would return the result in array.
sodasDf.printSchema 

// Now we are ready to apply SQL or any other processing on teh results

sodasDf.createOrReplaceTempView("sodastbl")

spark.sql("select source, region, inline(output) from sodastbl").show()


```


### Python API

This time we are reading the input data points from a csv file - sodainput.csv. The csv file contains two coloumns - 'region' and 'source'. And it has 3 rows with different values for these 2 columns. We shall call the API for these 3 sets of input data points with different values of 'region' and 'source'. The 'region' and 'source' are two filters supported by the SODA API for Socrata data source. 

The csv file should look like this -

region,source
Nevada,nn
Northern California,pr
Virgin Islands region,pr

Please ensure that the csv file doen not have any space in between the column names as well as in between the values for those columns in the rows.

```python

# Create the target url string for Soda API for Socrata data source
sodauri = 'https://soda.demo.socrata.com/resource/6yvf-kk3n.json'

# Now we are going to read the data from the csv file

sodainputDf = spark.read.option('header', 'true').csv('/home/biadmin/spark-enablement/datasets/sodainput.csv')

# And we create a temporary table now using the sodainputDf

sodainputDf.createOrReplaceTempView('sodainputtbl')

# Now we create the parameter map to pass to the Rest Data Source.

prmsSoda = { 'url' : sodauri, 'input' : 'sodainputtbl', 'method' : 'GET', 'readTimeout' : '10000', 'connectionTimeout' : '2000', 'partitions' : '10'}

# Now we create the Dataframe which contains the result from the call to the Soda API for the 3 different input data points

sodasDf = spark.read.format("org.apache.dsext.spark.datasource.rest.RestDataSource").options(**prmsSoda).load()

# We inspect the structure of the results returned. For Soda data source it would return the result in array.
sodasDf.printSchema() 

# Now we are ready to apply SQL or any other processing on teh results

sodasDf.createOrReplaceTempView("sodastbl")

spark.sql("select source, region, inline(output) from sodastbl").show()


```

### R API

We shall use the same csv file for the input data as in case of the Pythin example

```R

# Create the target url string for Soda API for Socrata data source
sodauri <- "https://soda.demo.socrata.com/resource/6yvf-kk3n.json"

# Now we are going to read the data from the csv file in a dataframe

sodainputDf <- read.df("/home/biadmin/spark-enablement/datasets/sodainput.csv", "csv", header = "true", inferSchema = "true", na.strings = "NA")

# And we create a temporary table now using the sodainputDf

createOrReplaceTempView(sodainputDf, "sodainputtbl")

# Now we create the Dataframe which contains the result from the call to the Soda API for the 3 different input data points. We pass teh necessary parameters.

sodasDf <- read.df(,"org.apache.dsext.spark.datasource.rest.RestDataSource", "url"=sodauri, "input"="sodainputtbl", "method"="GET")

# We inspect the structure of the results returned. For Soda data source it would return the result in array.
printSchema(sodasDf) 

# Now we are ready to apply SQL or any other processing on teh results

createOrReplaceTempView(sodasDf, "sodastbl")
sodas2df <- sql("select source, region, inline(output) from sodastbl")

head(sodas2df)

