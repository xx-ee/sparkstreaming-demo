package hbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}

object ReadHBaseDemo {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local").setAppName("readHbase")
    val sc=new SparkContext(conf)
    //--创建Hbase的环境变量参数
    val hbaseConf=HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum","hadoop0,hadoop1,hadoop2")
    hbaseConf.set("hbase.zookeeper.property.clientPort","2181")
    hbaseConf.set(TableInputFormat.INPUT_TABLE,"t2")
    val resultRDD=sc.newAPIHadoopRDD(hbaseConf,classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],classOf[Result])
    resultRDD.foreach{x=>{
      //--查询出来的结果集存在 (ImmutableBytesWritable, Result)第二个元素
      val result=x._2
      //--获取行键
      val rowKey=Bytes.toString(result.getRow)
      val name=Bytes.toString(result.getValue(Bytes.toBytes("cf1"),Bytes.toBytes("name")))
      val age=Bytes.toString(result.getValue(Bytes.toBytes("cf1"),Bytes.toBytes("age")))
      println(rowKey+":"+name+":"+age)
    }}
    println("*************end*************")
}
}
