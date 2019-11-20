package com.atguigu.orderpay_detect


import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector




object OrderTimeoutWithoutCep {

  val orderTimeoutOutTag = new OutputTag[OrderResult]("orderTimeout")

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val resource = getClass.getResource("/OrderLog.csv")
    val orderEventStream = env.readTextFile(resource.getPath)
      .map( data =>{
        val dataArray = data.split(",")
        OrderEvent(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong)
      })
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.orderId)

    //定义process function进行超时检测
    //val timeoutWarningSream = orderEventStream.process( new OrderTimeoutWarning())
    val orderResultStream = orderEventStream.process( new OrderPayMatch())

    orderResultStream.print("payed")
    orderResultStream.getSideOutput(orderTimeoutOutTag).print("timeout")
    env.execute("order timeout without cep job")


  }
  class OrderPayMatch() extends KeyedProcessFunction[Long,OrderEvent,OrderResult]{
    lazy val isPayedState:ValueState[Boolean] = getRuntimeContext.getState( new ValueStateDescriptor[Boolean]("ispayed-state",classOf[Boolean]))
    //保存定时器的时间戳为状态
    lazy val timeState:ValueState[Long] = getRuntimeContext.getState( new ValueStateDescriptor[Long](("timer-state"),classOf[Long]))
    override def processElement(value: OrderEvent, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context, out: Collector[OrderResult]): Unit = {
      //先读取状态
      val ispayed = isPayedState.value()
      val timerTs = timeState.value()

      //根据事件类型进行分类判断，做不同逻辑
      if (value.eventType == "create"){
        if (ispayed){
          //如果已经pay输出主流清空状态
          out.collect(OrderResult(value.orderId,"payed successfully"))
          ctx.timerService().deleteEventTimeTimer(timerTs)
          isPayedState.clear()
          timeState.clear()
        }else {
          //如果没有pay过，注册定时器等待pay的到来
          val ts = value.eventTime * 1000L + 15 * 60 * 1000L
          ctx.timerService().registerEventTimeTimer(ts)
          timeState.update(ts)
        }
      }else if (value.eventType == "pay"){
        //如果是pay事件，那么判断是否create过，用timer表示
        if(timerTs > 0){
          //如果有定时器，说明create过
          //继续判断
          if (timerTs > value.eventTime *1000L ){
            out.collect(OrderResult(value.orderId,"payed successfully"))
          }else{
            //如果当前pay的时间已经超时，那么输出到侧输出流
            ctx.output(orderTimeoutOutTag,OrderResult(value.orderId,"payed but already timeout"))
          }
          ctx.timerService().deleteEventTimeTimer(timerTs)
          isPayedState.clear()
          timeState.clear()
        }else{
          //pay先到了,更新状态，注册定时器 等待create
          isPayedState.update(true)
          ctx.timerService().registerEventTimeTimer(value.eventTime*1000L)
          timeState.update(value.eventTime*1000L)
        }
      }
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext, out: Collector[OrderResult]): Unit = {
      //根据状态的值判断哪个数据没来
      if (isPayedState.value()){
        //如果为真，表示pay先到了没等到create
        ctx.output(orderTimeoutOutTag,OrderResult(ctx.getCurrentKey,"already payed but not found create log"))

      }else{

        //表示create到了没等到pay
        ctx.output(orderTimeoutOutTag,OrderResult(ctx.getCurrentKey,"already create but not pay"))
      }
    }
  }

}
class OrderTimeoutWarning() extends KeyedProcessFunction[Long,OrderEvent,OrderResult] {
  //定义状态保存一个pay支付状态是否来过
  lazy val isPayedState:ValueState[Boolean] = getRuntimeContext.getState( new ValueStateDescriptor[Boolean]("ispayed",classOf[Boolean]))
  override def processElement(value: OrderEvent, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context, out: Collector[OrderResult]): Unit = {

    val isPayed = isPayedState.value()
    if (value.eventType == "create" && !isPayed){
      ctx.timerService().registerEventTimeTimer(value.eventTime *1000L + 15 * 60 *1000L)
    }else if (value.eventType == "pay") {
      isPayedState.update(true)

    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext, out: Collector[OrderResult]): Unit = {
    val isPayed = isPayedState.value()
    if (isPayed){
      out.collect(OrderResult(ctx.getCurrentKey,"order payed successfully"))
    }else{
      out.collect(OrderResult(ctx.getCurrentKey,"order timeout"))
    }
    isPayedState.clear()
  }
}
