package com.etiantian

import java.io.{File, FileInputStream}
import java.text.SimpleDateFormat
import java.util.{Date, Properties, UUID}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.log4j._
import org.apache.spark.rdd.{PairRDDFunctions, RDD}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json.JSONObject
import redis.clients.jedis.Jedis

/**
 * Hello world!
 *
 */
object MessageHandler {
  def main(args: Array[String]): Unit = {
    if (args == null || args.length < 1) {
      Console.err.print("Missing a parameter: Properties' path")
    }
    val file = new File(args(0).substring(0, args(0).lastIndexOf("/")) + "/log4j.properties")
    if (file.exists()) {
      PropertyConfigurator.configure(args(0).substring(0, args(0).lastIndexOf("/")) + "/log4j.properties")
    }
    val logger = Logger.getLogger("MessageHandler")


    val configFile = new File(args(0))
    if (!configFile.exists()) {
      logger.error("Missing config.properties file!")
    }

    val properties = new Properties()
    properties.load(new FileInputStream(configFile))

    val quorum = properties.getProperty("kafka.quorum")
    val sparkConf = new SparkConf()
    val ssc = new StreamingContext(sparkConf, Seconds(properties.getProperty("streaming.cycle").toInt))
    val sc: SparkContext = ssc.sparkContext
    val map = Map("aixueOnline" -> 1)

    val kafkaStream = KafkaUtils.createStream(ssc, quorum, "aixueMessage", map)

    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    ssc.checkpoint("/tmp/checkpoint/" + ssc.sparkContext.appName)

    /**
      * 计算在线用户数量
      */
    val vTime = sc.accumulator[Long](0)
    val vCount = sc.accumulator[Long](0)
    kafkaStream.foreachRDD(rdd => {

      val time = format.format(new Date())

      vTime.value = 0l
      vCount.value = 0l
      rdd.foreach(line => {
        val message = line._2

        try {
          val json = new JSONObject(message)
          if (json.has("jid") && json.has("cost_time")) {
            vCount.add(1);
            vTime.add(json.get("cost_time").toString.toLong)
          }
        } catch {
          case ex: Exception => {
            println(ex.getMessage)
          }
        }
      })

      val userCount = rdd.map(line => {
        val message = line._2
        var jid: String = null
        try {
          val json = new JSONObject(message)
          if (json.has("jid")) {
            jid = json.get("jid").toString
          }
        } catch {
          case ex: Exception => {
            println(ex.getMessage)
          }
        }
        (jid, 1)
      }).filter(_._1 != null).reduceByKey(_ + _).count


      val interTuple = rdd.map(line => {
        val message = line._2
        var url = ""
        var interName: String = null
        var costTime = 0l
        try {
          val json = new JSONObject(message)
          if (json.has("url") && json.has("cost_time")) {
            url = json.get("url").toString
            if (url.indexOf(".do") != -1) {
              val mbegin: Int = url.substring(0, url.indexOf(".do")).lastIndexOf("/") + 1
              val mend: Int = url.indexOf(".do") + 3
              interName = url.substring(mbegin, mend)
            }
            else {
              var mbegin: Int = url.indexOf("m=")
              if (mbegin == -1) mbegin = url.indexOf("?") + 1
              var mend: Int = url.substring(mbegin).indexOf("&")
              if (mend == -1) mend = url.substring(mbegin).length

              var aend: Int = url.indexOf("?")
              if (aend == -1) aend = url.length
              val abegin: Int = url.substring(0, aend).lastIndexOf("/") + 1

              interName = url.substring(abegin, aend) + "?" + url.substring(mbegin).substring(0, mend)
            }

            costTime = json.get("cost_time").toString.toLong
          }
        } catch {
          case ex: Exception => {
            println(ex.getMessage)
          }
        }
        (interName, costTime)
      }).filter(tuple => tuple._1 != null)

      val interSumTuple = interTuple.reduceByKey(_ + _).map(tuple => (tuple._1 + "_sum", tuple._2.toString))

      val interCount = interSumTuple.count()

      /**
        * 写入redis
        */
      val jedis = new Jedis(properties.getProperty("redis.hostName"), properties.getProperty("redis.port").toInt)
      jedis.hset("aixue", "onlineUser", userCount.toString)
      jedis.hset("aixue", "visitCount", vCount.value.toString)


      jedis.hset("aixue", "sumTime", vTime.value.toString)
      var avgTime = 0
      if (vCount.value > 0) {
        avgTime = (vTime.value / vCount.value).toInt
      }
      jedis.hset("aixue", "avgTime", avgTime.toString)

      jedis.hset("aixue", "interCount", interCount.toString)

      val conf = new Configuration()
      conf.set("hbase.zookeeper.quorum", properties.getProperty("hbase.quorum"))
      conf.set("hbase.zookeeper.property.clientPort", properties.getProperty("hbase.zkPort"))
      conf.set(TableOutputFormat.OUTPUT_TABLE, "aixue_online")

      val job = Job.getInstance(conf)
      job.setOutputKeyClass(classOf[ImmutableBytesWritable])
      job.setOutputValueClass(classOf[Put])
      job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

      val dataList = List((time, userCount, vCount.value, vTime.value, avgTime, interCount))

      rdd.sparkContext.parallelize(dataList).map(tuple => {
        val put = new Put(Bytes.toBytes(tuple._1))
        put.addColumn(Bytes.toBytes("aixue"), Bytes.toBytes("onlineUser"), Bytes.toBytes(tuple._2.toString))
        put.addColumn(Bytes.toBytes("aixue"), Bytes.toBytes("visitCount"), Bytes.toBytes(tuple._3.toString))
        put.addColumn(Bytes.toBytes("aixue"), Bytes.toBytes("sumTime"), Bytes.toBytes(tuple._4.toString))
        put.addColumn(Bytes.toBytes("aixue"), Bytes.toBytes("avgTime"), Bytes.toBytes(tuple._5.toString))
        put.addColumn(Bytes.toBytes("aixue"), Bytes.toBytes("interCount"), Bytes.toBytes(tuple._6.toString))
        (new ImmutableBytesWritable, put)
      }).saveAsNewAPIHadoopDataset(job.getConfiguration)

      jedis.expire("aixue", properties.getProperty("streaming.cycle").toInt + 1)
      jedis.close()
    })


    ssc.start()
    ssc.awaitTermination()
  }
}
