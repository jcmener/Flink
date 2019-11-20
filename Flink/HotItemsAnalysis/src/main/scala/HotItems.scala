
import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer


case class UserBehavior(userId: Long, itemsId: Long, categoryId: Int, behavior: String, timestamp: Long)

case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)

object HotItems {
  def main(args: Array[String]): Unit = {
    //创造一个env
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //显式的定义Time类型
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //设置并发为1
    env.setParallelism(1)

    val stream = env
      .readTextFile("G:\\BigDate\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
      .map(line => {
        val linearray = line.split(",")
        UserBehavior(linearray(0).toLong,linearray(1).toLong,linearray(2).toInt,linearray(3),linearray(4).toLong)
      //指定时间戳和watermark
      })
      .assignAscendingTimestamps(_.timestamp*1000)
        .filter(_.behavior == "pv")
        .keyBy(_.itemsId)
        .timeWindow(Time.hours(1),Time.minutes(5))
        .aggregate(new CountAgg(),new WindowResultFunction())
        .keyBy(_.windowEnd)
        .process(new TopNHotItems(3))
        .print()
    //调用execute执行任务
    env.execute("Hot Items Job")
  }



  class CountAgg() extends AggregateFunction[UserBehavior, Long, Long] {
    override def createAccumulator(): Long =0L

    override def add(in: UserBehavior, acc: Long): Long = acc+1

    override def getResult(acc: Long): Long = acc

    override def merge(acc: Long, acc1: Long): Long = acc+acc1
  }



   class WindowResultFunction() extends WindowFunction[Long, ItemViewCount, Long, TimeWindow]{
     override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
       out.collect(ItemViewCount(key,window.getEnd,input.iterator.next()))
     }
   }



  class TopNHotItems(topSize: Int) extends KeyedProcessFunction[Long,ItemViewCount,String] {
    //定义状态ListState
    private var itemState:ListState[ItemViewCount] = _

    override def open(parameters: Configuration): Unit = {
      super.open(parameters)
      //命名状态变量的名字和类型
      val itemStateDesc = new ListStateDescriptor[ItemViewCount]("itemState",classOf[ItemViewCount])
      itemState = getRuntimeContext.getListState(itemStateDesc)
    }

    override def processElement(i: ItemViewCount, context: KeyedProcessFunction[Long, ItemViewCount, String]#Context, collector: Collector[String]): Unit = {
      itemState.add(i)

      //注册定时器，触发时间设置成windowEnd+1，触发时说明window已经收集完成所有数据
      context.timerService.registerEventTimeTimer(i.windowEnd+1)
    }


    //定时器触发操作，从state里取出所有数据，排序取topN，输出
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
      super.onTimer(timestamp, ctx, out)
      val allItems:ListBuffer[ItemViewCount] = ListBuffer()
      import scala.collection.JavaConversions._
      for (item <- itemState.get){
        allItems += item
      }
      //清除状态数据，释放空间
      itemState.clear()
      //按照点击量从大到小排序，选取topN
      val sortedItems = allItems.sortBy(_.count)(Ordering.Long.reverse).take(topSize)
      //将排名数据格式化便于打印输出
      val result:StringBuilder = new StringBuilder()

      result.append("====================================\n")
      result.append("时间：").append(new Timestamp(timestamp-1)).append("\n")

      for (i <- sortedItems.indices){
        val currentItem:ItemViewCount = sortedItems(i)
        // e.g.  No1：  商品ID=12224  浏览量=2413
        result.append("No").append(i+1).append(":")
          .append("  商品ID=").append(currentItem.itemId)
          .append("  浏览量=").append(currentItem.count).append("\n")
      }
      result.append("====================================\n\n")
      // 控制输出频率，模拟实时滚动结果
      Thread.sleep(1000)
      out.collect(result.toString)

    }
  }

}
