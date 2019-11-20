package com.atguigu.marketanalysis

import java.sql.Timestamp
import java.util.UUID
import java.util.concurrent.TimeUnit
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

case class MarketingUserBehavior(userId:String,behavior:String,channel:String,timestamp:Long)

case  class MarketingViewCount(windowStart:String,windowEnd:String,channel:String,behavior:String,timestamp:Long)

object AppMarketingByChannel {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream = env.addSource(new SimulatedEventSource())
      .assignAscendingTimestamps(_.timestamp)
      .filter(_.behavior != "Unistall")
      .map(data => {
        ((data.channel,data.behavior),1L)
      })
      .keyBy(_._1)
      .timeWindow(Time.hours(1),Time.seconds(10))
      .process( new MarketingCountByChannel())

    dataStream.print()
    env.execute("app marketing by channel job")

  }

  //自定义数据源
  class SimulatedEventSource() extends RichSourceFunction[MarketingUserBehavior] {

    //定义是否运行的标识类
    var running = true
    //定义用户行为集合
    val behaviorTypes :Seq[String] = Seq("Click","Download","Install","Unistall")
    //定义渠道集合
    val channelSets :Seq[String] = Seq("wechat","webo","appstore","huaweistore")
    //定义一个随机数发生器
    val rand:Random = new Random()

    override def run(ctx: SourceFunction.SourceContext[MarketingUserBehavior]): Unit = {
      //定义一个生气数据的上限
      val maxElements = Long.MaxValue
      var count = 0L

      //随机生成所有数据
      while (running && count < maxElements){
        val id = UUID.randomUUID().toString
        val behavior = behaviorTypes(rand.nextInt(behaviorTypes.size))
        val channel = channelSets(rand.nextInt(channelSets.size))
        val ts = System.currentTimeMillis()

        ctx.collect(MarketingUserBehavior(id,behavior,channel,ts))

        count += 1
        TimeUnit.MILLISECONDS.sleep(10L)

      }

    }

    override def cancel(): Unit = running =false
  }

  //自定义处理函数
  class MarketingCountByChannel() extends ProcessWindowFunction[((String,String),Long),MarketingViewCount,(String,String),TimeWindow] {

    override def process(key: (String, String), context: Context, elements: Iterable[((String, String), Long)], out: Collector[MarketingViewCount]): Unit = {
      val startTs = new Timestamp(context.window.getStart).toString
      val endTs = new Timestamp(context.window.getEnd).toString
      val channel = key._1
      val behavior = key._2
      val count = elements.size

      out.collect(MarketingViewCount(startTs,endTs,channel,behavior,count))

    }
  }

}
