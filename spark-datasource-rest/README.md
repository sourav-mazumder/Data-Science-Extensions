# Rest Data Source for Apache Spark

- This is a library for calling REST based services/APIs for multiple sets of input parameters in parallel and collating the results, returned by the REST service, in a Dataframe.

- REST based services (for example Address Validation services, Google Search API, Watson Natural Language Processing API, etc.) typically take only one set of input parameters at a time and return the corresponding record(s). However, for many Data Science problems, the same API needs to be called multiple times to account for a large set of different input parameters (e.g. validating the addresses for a set of target customers; getting personality insights from thousands of Tweets; obtaining Provider's details from the NPI registry for a list of potential Doctors, etc.

- This package supports calling the target service API in a distributed way for different input parameter sets. The results are returned in a DataFrame in a Structure specific to the API without the user specifying this schema.

## Requirements

This library requires Spark 2.0+ .

## Dependency

This library depends on [scalaj_http](https://github.com/scalaj/scalaj-http) package

## Accessing the binary/jar file for this library already available in the release

You can try out the binary/jar file for this library by downloading it from the the Release section of the parent repositoty Data-Science-Extensions.

Click to the name of the repository Data-Science_Extensions. Go to the the Release link under the name of the repositoty. ONce you click there you would fine the link for the file spark-datasource-rest_2.11-2.1.0-SNAPSHOT.jar.zip. Download this file. Uncompress the same. Now you have spark-datasource-rest_2.11-2.1.0-SNAPSHOT.jar to use in your Spark Cluster.


## Building the jar file

In case you want to build the jar file yourself try the following steps. 

Clone/Download the master repository (Data-Science-Extensions) in your local system. Go the parent folder (the one which has 'spark-datasource-rest' as one of the sub folders). From the parent older run the command below. This command will create a 'target' folder under the folder 'spark-datasource-rest'. The 'target' folder would have the spark-datasource-rest_2.11-2.1.0-SNAPSHOT.jar which now you can use with any Spark cluster.

```
mvn clean install -pl spark-datasource-rest -DskipTests -Dscalastyle.skip=true
```

## Using the jar file with Spark shell
This package can be added to  Spark using the `--packages` of `--jars` command line option.  For example, to include it when starting the spark shell as jar from the same folder use :

```
$SPARK_HOME/bin/spark-shell --jars spark-datasource-rest_2.11-2.1.0-SNAPSHOT.jar --packages org.scalaj:scalaj-http_2.10:2.3.0

```

## Features
This package allows multiple calls, in parallel, to a target REST based micro service for a set of different input parameters. These parameters can be passed as a Temporary Spark Table where the column names of the table should be same as the keys of the target API. Each row in the table, and corresponding combination of parameter values, will be used to make one API call. The result from the multiple calls to the API is returned as a [Spark DataFrame](https://spark.apache.org/docs/1.6.0/sql-programming-guide.html) of Rows with an output structure matching that of the target API's response.  

This library supports several options for calling the target REST service:
* `url`: This is the uri of the target micro service. You can also provide the common parameters (those that don't vary with each API call) in this url. This is a mandatory parameter.
* `input`: You need to pass the name of the Temporary Spark Table which contains the input parameters set. This is a mandatory parameter too.
* `method`: The supported http/https method. Possible types supported right now are `POST`, and `GET`. Default is `POST`
* `userId` : The userId in case the target API needs basic authentication.
* `userPassword` : The password in case the target API needs basic authentication
* `partitions`: Number of partition to be used to increase parallelism. Default is 2.
* `connectionTimeout` : In case the target API needs high time to connect. Default is 1000 (in ms)
* `readTimeout` : In case the target API returns large volume of data which needs more read time. Default is 5000 (in ms)
* `schemaSamplePcnt` : Percentage of records in the input table to be used to infer the schema. The default is "30" and minimum is 3. Increase this number in case you are getting an error or the schema is not propery inferred.
* `callStrictlyOnce` : This value can be used to ensure the backend API is called only once for each set of input parameters. The default is "N", allowing the back end API to get called for multiple times - once for inferring the schema and then for other operations. If this value is set to "Y" the backend API would be called only once (during infering the schema) for all of the input parameter sets and would be cached. This option is useful when the target APIs are paid service or does not support calls per day/per hour beyond certain number. However, the results would be cached which will increase the memory usage.

## Typical Structure of the Dataframe returned by Rest Data Source

The dataframe created by this REST Data Source will return a set of Rows of the same Structure. The Structure will contain the input fields that were used for the API call as well as the returned output under a new column named 'output'. Whatever gets returned in 'output' is specific to the target REST API. It's structure can be easily obtained by printSchema method of Dataframe.

Below is an example of the structure returned using REST Data Source with the [Watson API for Natural Language Understanding with sentiment as feature] (https://www.ibm.com/watson/developercloud/natural-language-understanding/api/v1/).

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
 
In this example, 'q' and 'url' are the input parameters passed to the service; the Temporary Spark Table used as input had two columns - 'q' and 'url'. Each row of that table had different values for 'q' and 'url'. The 'output' field contains the result returned by the Watson API for Natural Language Understanding.  

Sometimes there could be an additional field under root, namely '\_corrupt_records'. This field will contain the outputs for the records for which the API returned an error.


## Examples (to try out with Spark Shell)

The examples below shows how to use this REST Data Source with a SODA API to retrive a Socrata dataset. The columns in the Socrata dataset have corresponding fields in the SODA API. Records are searched for using filters and [SoQL queries](https://dev.socrata.com/docs/queries/). The below examples demonstrate how to use the filters.


### Scala API

```scala
// Create the target url string for Soda API for Socrata data source
val sodauri = "https://soda.demo.socrata.com/resource/6yvf-kk3n.json"

///Say we need to call the API for 3 sets of input parameters for different values of 'region' and 'source'. The 'region' and 'source' are two filters supported by the SODA API for Socrata data source

val sodainput1 = ("Nevada", "nn")
val sodainput2 = ("Northern California", "pr")
val sodainput3 = ("Virgin Islands region", "pr")

// Now we create a RDD using these input parameter values

val sodainputRdd = sc.parallelize(Seq(sodainput1, sodainput2, sodainput3))

// Next we need to create the DataFrame specifying specific column names that match the field names we wish to filter on
val sodainputKey1 = "region"
val sodainputKey2 = "source"

val sodaDf = sodainputRdd.toDF(sodainputKey1, sodainputKey2)

// And we create a temporary table now using the sodaDf
sodaDf.createOrReplaceTempView("sodainputtbl")

// Now we create the parameter map to pass to the REST Data Source.

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

This time we are reading the input parameter values from a csv file - sodainput.csv. The csv file contains two columns - 'region' and 'source'. This column names map to two filters supported by the SODA API for Socrata data source and do not require renaming. We shall call the API 3 times, one for each of the different combinations of 'region' and 'source' values. 

The csv file should look like this -

region,source  
Nevada,nn  
Northern California,pr  
Virgin Islands region,pr  


Please ensure that the csv file does not have any space in between the column names as well as in between the values for those columns in the rows.

```python

# Create the target url string for Soda API for Socrata data source
sodauri = 'https://soda.demo.socrata.com/resource/6yvf-kk3n.json'

# Now we are going to read the data from the csv file

sodainputDf = spark.read.option('header', 'true').csv('/home/biadmin/spark-enablement/datasets/sodainput.csv')

# And we create a temporary table now using the sodainputDf

sodainputDf.createOrReplaceTempView('sodainputtbl')

# Now we create the parameter map to pass to the REST Data Source.

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

We shall use the same csv file for the input parameter values from the Python example

```R

# Create the target url string for Soda API for Socrata data source
sodauri <- "https://soda.demo.socrata.com/resource/6yvf-kk3n.json"

# Now we are going to read the data from the csv file in a dataframe

sodainputDf <- read.df("/home/biadmin/spark-enablement/datasets/sodainput.csv", "csv", header = "true", inferSchema = "true", na.strings = "NA")

# And we create a temporary table now from sodainputDf

createOrReplaceTempView(sodainputDf, "sodainputtbl")

# Now we create the Dataframe which contains the result from the call to the Soda API for the 3 different input data points. We pass teh necessary parameters.

sodasDf <- read.df(,"org.apache.dsext.spark.datasource.rest.RestDataSource", "url"=sodauri, "input"="sodainputtbl", "method"="GET")

# We inspect the structure of the results returned. For Soda data source it would return the result in an array.
printSchema(sodasDf) 

# Now we are ready to apply SQL or any other processing on the results

createOrReplaceTempView(sodasDf, "sodastbl")
sodas2df <- sql("select source, region, inline(output) from sodastbl")

head(sodas2df)

``` 


## Using Rest Data Source in IBM Data Science Experience (DSx)

Here is a quick way to try out this Data Source using free version of IBM Data Science Experience

1. At first get your free account in Data Scienece Experience (DSx)

Use this [link](https://datascience.ibm.com/) to get your free account for Data Science Experience. It by default comes with a Spark Cluster where you can try out this Data Source. This also automatically creates an IBM Cloud account for you

2. Next Create your first project in Data Scienece Experience (DSx)

Go to teh Get Started link and create a new Project. While creating a project it will also create a new Spark Service. Note the name of the Spark Service.

3. Get credential of the Spark As A Service

Login to IBM Cloud. Check the Dashboard. You should see the name of your Spark Service (the SErice Offering would Apache Spark). 

Click your Spark Service. This will open the window which has a link called Service CRedentuial in the left pane. Click that and it will take you to the Credential Window. There youb shall see the available Service Credentials. Click 'View Credential'. This will show you the credentials in a json format. Copy the json string for using in next step.

4. Upload the jar file to Data Scienece Experience (DSx)

At first, download the jar file for this library from the release link of the parent repository Data-Science-Expecrience. Follow the steps those have been outlined in the section 'Accessing the binary/jar file for this library already available in the release' of this document. 

Alternatively, you can create the library by following the steps mentioned in the section 'Building the jar file' of this document. 

Next, you can upload the jar file to Data Science Experience by followng the guidance in this [link](https://console.bluemix.net/docs/services/AnalyticsforApacheSpark/spark_environment_example.html#example-optional-file-transfer-and-environment-configuration)

The typical command for the upload would look like as below. The value of tenant_id, tenant_secret, instance_id and cluster_master_url you can get from teh credential json you got in last step. The spark-datasource-rest_2.11-2.1.0-SNAPSHOT.jar is the file got created when you run the build instruction in the target folder. You need to run the command below from the target folder. This would upload the jar file to the Spark Instance in your DSx project
```
curl \
    -X PUT \
    -k \
    -u ${tenant_id}:${tenant_secret} \
    -H "X-Spark-service-instance-id: ${instance_id}" \
    --data-binary "@./spark-datasource-rest_2.11-2.1.0-SNAPSHOT.jar" \
    ${cluster_master_url}/tenant/data/libs/spark-datasource-rest_2.11-2.1.0-SNAPSHOT.jar" \
 ```
 5. Using this jar in the Notebooks in your DSx project
 
 Now go to DSx andunder the project you created create a new Notebook. You can create either a Scala, Python or R notebook.
 Now in that notebook use this jar to call the Rest APIs. 
 
 Here is a link to an example Python [Notebook](https://dataplatform.ibm.com/analytics/notebooks/ae63f056-e267-443e-bfc0-b9331f51d68a/view?access_token=0ec63c6e031aa57d065a4e1c4b71733729db43b1490c331a44323cce28725b7d). This notebook shows how to use the Rest Data Source for calling Socrata Open Data API (SODA) data service
    
