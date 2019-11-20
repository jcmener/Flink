package com.atguigu.orderpay_detect

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

case class ReceiptEvent(txId:String, payChannel:String, eventTime:Long)
object TxMacthDetect {

  //定义侧输出流
  val unmatchedPays = new OutputTag[OrderEvent]("unmatchedPays")

  val unmatchedReceipts = new OutputTag[ReceiptEvent]("unmatchReceipts")

  def main(args: Array[String]): Unit = {
    val env =StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val resource = getClass.getResource("/OrderLog.csv")
    val orderEventStream = env.readTextFile(resource.getPath)
      .map( data =>{
        val dataArray = data.split(",")
        OrderEvent(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong)
      })
      .filter(_.txId != " ")
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.txId)

    //读取支付到账事件流
    val receiptResource = getClass.getResource("/ReceiptLog.csv")
    val receiptEventStream = env.readTextFile(receiptResource.getPath)
      .map( data =>{
        val dataArray = data.split(",")
        ReceiptEvent(dataArray(0).trim, dataArray(1).trim, dataArray(2).toLong)
      })
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.txId)

    //将两条流连接起来，共同处理
    val processedStream = orderEventStream.connect(receiptEventStream)
      .process(new TxPayMatch())

    processedStream.print("matched")
    processedStream.getSideOutput(unmatchedPays).print("unmatchedPays")
    processedStream.getSideOutput(unmatchedReceipts).print("unmatchedReceipts")

    env.execute("tx")
  }
  class TxPayMatch() extends CoProcessFunction[OrderEvent,ReceiptEvent,(OrderEvent,ReceiptEvent)]{

    //定义状态 来保存已经到达的订单支付事件和状态事件
    lazy val payState:ValueState[OrderEvent] = getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("pay-state",classOf[OrderEvent]))
    lazy val receiptState:ValueState[ReceiptEvent] = getRuntimeContext.getState(new ValueStateDescriptor[ReceiptEvent]("receipt-state",classOf[ReceiptEvent]))

    //订单支付事件数据处理
    override def processElement1(value: OrderEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {

      //判断有没有到账事件
      val receipt = receiptState.value()
      if (receipt != null){
        //如果已经有receipt，在主流输出匹配信息
        out.collect((value,receipt))
        receiptState.clear()
      }else{
        //如果还没到 把pay存入状态 做一个定时器等待
        payState.update(value)
        ctx.timerService().registerEventTimeTimer(value.eventTime* 1000L + 5000L)
      }
    }

    //到账事件处理
    override def processElement2(value: ReceiptEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {

      //判断支付事件
      val pay = payState.value()
      if (pay != null){
        out.collect((pay,value))
        receiptState.clear()
      }else{
        receiptState.update(value)
        ctx.timerService().registerEventTimeTimer(value.eventTime * 1000L + 5000L)
      }
    }

    override def onTimer(timestamp: Long, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#OnTimerContext, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      //到时间了 如果还没收到某个事件，那么输出报警
      if(payState.value() != null){
        //receipt  没来
        ctx.output(unmatchedPays,payState.value())
      }
      if (receiptState.value() != null){
        ctx.output(unmatchedReceipts,receiptState.value())
      }
      payState.clear()
      receiptState.clear()
    }
  }
}
