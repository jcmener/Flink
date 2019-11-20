package com.atguigu.networkflow_analysis



import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer


//输入数据样例类
case class ApacheLogEvent(ip:String,userId:String,eventTime:Long,method:String,url:String)

//窗口聚合结果样例类
case class UrlViewCount(url:String,windowEnd:Long,count:Long)

object NetworkFlow {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dateStream = env.readTextFile("G:\\BigDate\\NetworkFlowAnalysis\\src\\main\\resources\\apache.log")
      .map(data => {
        val dataArray = data.split(" ")
        //订一时间转换
        val simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val timestamp = simpleDateFormat.parse(dataArray(3).trim).getTime
        ApacheLogEvent(dataArray(0).trim,dataArray(1).trim,timestamp,dataArray(5).trim,dataArray(6).trim)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(60)){
        override def extractTimestamp(element: ApacheLogEvent): Long = element.eventTime
      })
      .keyBy(_.url)
      .timeWindow(Time.minutes(10),Time.seconds(5))
      .allowedLateness(Time.seconds(60))
      .aggregate(new countAgg,new windowResult)
      .keyBy(_.windowEnd)
      .process(new TopNHotUrls(5))

    dateStream.print()
    env.execute("NetworkFlow")


  }

  class countAgg() extends AggregateFunction[ApacheLogEvent,Long,Long]{
    override def createAccumulator(): Long = 0L

    override def add(in: ApacheLogEvent, acc: Long): Long = acc + 1

    override def getResult(acc: Long): Long = acc

    override def merge(acc: Long, acc1: Long): Long = acc + acc1
  }

  class windowResult() extends WindowFunction[Long,UrlViewCount,String,TimeWindow] {
    override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
      out.collect(UrlViewCount(key,window.getEnd,input.iterator.next()))
    }
  }


  class TopNHotUrls(i: Int) extends KeyedProcessFunction[Long,UrlViewCount,String]{
    lazy val urlState:ListState[UrlViewCount] =getRuntimeContext.getListState(new ListStateDescriptor[UrlViewCount]("url-state",classOf[UrlViewCount]))

    override def processElement(value: UrlViewCount, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#Context, out: Collector[String]): Unit = {
      urlState.add(value)
      ctx.timerService().registerEventTimeTimer(value.windowEnd+1)



    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
      //从状态中拿出数据
      val allUrlViews:ListBuffer[UrlViewCount] = new ListBuffer[UrlViewCount]()
      val iter = urlState.get().iterator()
      while(iter.hasNext){
        allUrlViews += iter.next()
      }

      urlState.clear()
      val sorteUrlViews = allUrlViews.sortWith(_.count > _.count).take(i)

      val result = new StringBuilder()
      result.append("时间:").append(new Timestamp(timestamp - 1)).append("\n")
      for (i <-  sorteUrlViews.indices){
        val currentUrl = sorteUrlViews(i)
        result.append("No.").append(i+1).append(":")
        result.append("URL=").append(currentUrl.url)
          .append("访问量:").append(currentUrl.count).append("\n")
      }
      result.append("======================================")
      Thread.sleep(1000)
      out.collect(result.toString())
    }


  }

}
