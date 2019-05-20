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

package org.apache.dsext.spark.datasource.rest

import java.nio.charset.StandardCharsets

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.{StringType, StructType, StructField}
import org.apache.spark.sql.sources.{BaseRelation, InsertableRelation, TableScan}


/*
   This class creates the relation out of the results obtained from the calls to Rest service
 */


case class RESTRelation(
    restOptions: RESTOptions)(@transient val sparkSession: SparkSession)
  extends BaseRelation
  with TableScan
  with InsertableRelation {

  override def sqlContext: SQLContext = sparkSession.sqlContext

  override val needConversion: Boolean = true

  override def buildScan() : RDD[Row] = {

    // print("in buildScan " + "\n")
    sparkSession.read.schema(schema).json(restRdd).rdd

  }

  override def insert(data: Dataset[Row], overwrite: Boolean): Unit = {}

  override def toString: String = {

    s"RESTRelation(${restOptions.url})"

  }

  private val inputs = restOptions.input
  private val inputKeys = restOptions.inputKeys

  private val inputDf = if (restOptions.inputType == "tableName") {
        sparkSession.sql(s"select *  from $inputs")
  }
  else {
        import sparkSession.implicits._
        val inputStrArr = inputs.split(restOptions.inputStrRecordDelimeter)
        sparkSession.sparkContext.parallelize(inputStrArr).toDF("strInput")
  }

  private val columnNames : Array[String] = inputDf.columns

  private val restRdd : RDD[String] = {

    val parts = restOptions.inputPartitions.toInt
    // print("in restRdd  : " + parts +   "\n")
    inputDf.rdd.repartition(parts).map(r => callRest(r))

  }

  override  val schema: StructType = {

    if (restOptions.callStrictlyOnce == "Y") {
      // print("in schema for strictly once  " +  "\n")
      sparkSession.read.json(restRdd.persist(StorageLevel.MEMORY_AND_DISK_SER)).schema
    }
    else {
      import sparkSession.implicits._
      val samplePcnt : Double = restOptions.schemaSamplePcnt.toInt/100.00
      val sampleDf = inputDf.sample(false, samplePcnt)
      val finalSampleDf = if (sampleDf.count < 3) inputDf.limit(3) else sampleDf
      // print("in schema  - not a strictOnce case - sample pcnt : "
      // + samplePcnt + " , sample count : " + sampleDf.count +
      //  ", final sample count : " + finalSampleDf.count + "\n")
      val outRdd = finalSampleDf.rdd.map(r => callRest(r)).asInstanceOf[RDD[String]]
      sparkSession.read.json(outRdd).schema
    }

  }

  private def callRest(data : Row) : String = {

    val valArray = data.toSeq.toArray.map(_.toString)

    val valuesArr = if (restOptions.inputType == "tableName") valArray else {
        valArray(0).split(restOptions.inputStrFieldDelimeter)
    }

    val inputDataStr = prepareInputData(valuesArr)

    val contentType = "application/" + restOptions.postInputFormat
    val userCred = if (restOptions.userId == "") ""
        else restOptions.userId + ":" + restOptions.userPassword
    val connectionStr = restOptions.connectionTimeout + ":" + restOptions.readTimeout
    val oauthStr = if (restOptions.oauthConsumerKey == "") "" else {
        (restOptions.oauthConsumerKey + ":" + restOptions.oauthConsumerSecret
           + ":" + restOptions.oauthToken + ":" + restOptions.oauthTokenSecret)
    }

    // print("in callRest input data str : " + inputDataStr +
    //  ", contentType : " + contentType + "\n")

    val resp = RestConnectorUtil.callRestAPI(restOptions.url, inputDataStr,
           restOptions.method, oauthStr, userCred, connectionStr,
           contentType, "BODY", restOptions.oauthToken).asInstanceOf[String]
    prepareOutputData(valuesArr, resp)

  }

  private def prepareInputData(valArray: Array[String]) : String = {

    val inputDataKeys = restOptions.inputKeys
    val keyArr = if (inputDataKeys == "") columnNames else inputDataKeys.split(",")

    if(restOptions.method == "GET") {
        RestConnectorUtil.prepareTextInput(keyArr, valArray)
    }
    else restOptions.postInputFormat match {
        case "json" => RestConnectorUtil.prepareJsonInput(keyArr, valArray)
        case "xml" => throw new Exception("XML based input for post is not supported yet")
        case _ => throw new Exception("Only JSON based input for post is supported now")
    }

  }

  private def prepareOutputData(valArray: Array[String], outputStr: String) : String = {

    val includeInputFlg = restOptions.includeInputsInOutput

    val inputDataKeys = restOptions.inputKeys
    val keyArr = if (inputDataKeys == "") columnNames else inputDataKeys.split(",")

    if(includeInputFlg == "N") outputStr else {

        restOptions.outputFormat match {
           case "json" => RestConnectorUtil.prepareJsonOutput(keyArr, valArray, outputStr)
           case "xml" => throw new Exception("XML output including Input keys is not supported yet")
           case "csv" => throw new Exception("CSV output including Input keys is not supported yet")
           case  _ => throw new Exception("Only JSON  output including Input keys is supported now")
        }

    }

  }

}
