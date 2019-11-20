package com.atguigu.networkflow_analysis


import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time


case class UserBehavior(userId: Long, itemsId: Long, categoryId: Int, behavior: String, timestamp: Long)


object PageView {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    //用相对路径定义数据源
    val resource = getClass.getResource("/UserBehavior.csv")
    val dataStream = env.readTextFile(resource.getPath)
      .map( data =>{
        val dataArray = data.split(",")
        UserBehavior(dataArray(0).toLong,dataArray(1).toLong,dataArray(2).toInt,dataArray(3),dataArray(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)
      .filter(_.behavior == "pv")
      .map(data =>("pv",1))
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .sum(1)

    dataStream.print("pv count")
    env.execute("PageView")



  }
}
