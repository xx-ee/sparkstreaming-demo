package hbase

import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}

object WriteHBaseDemo {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local").setAppName("writeHbase")
    val sc=new SparkContext(conf)

    sc.hadoopConfiguration.set("hbase.zookeeper.quorum","hadoop0,hadoop1,hadoop2")
    sc.hadoopConfiguration.set("hbase.zookeeper.property.clientPort","2181")
    sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE,"t2")
    val job=new Job(sc.hadoopConfiguration)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

    val data=sc.makeRDD(Array("rk1,tom,23","rk2,rose,25","rk3,jary,30"))
    val hbaseRDD=data.map { line =>{
      val infos=line.split(",")
      val rowKey=infos(0)
      val name=infos(1)
      val age=infos(2)

      val put=new Put(Bytes.toBytes(rowKey))
      put.add(Bytes.toBytes("cf1"),Bytes.toBytes("name"),Bytes.toBytes(name))
      put.add(Bytes.toBytes("cf1"),Bytes.toBytes("age"),Bytes.toBytes(age))

      (new ImmutableBytesWritable,put)
    } }

    //--将RDD数据存储进Hbase
    hbaseRDD.saveAsNewAPIHadoopDataset(job.getConfiguration)
    println("*****************end*****************")

  }
}
