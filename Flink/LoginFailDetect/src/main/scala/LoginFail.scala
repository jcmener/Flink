import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector



import scala.collection.mutable.ListBuffer

//输入登录事件样例类
case class LoginEvent(userId:Long, ip:String, eventType:String, eventTime:Long )

//输出的异常报警信息样例类
case class Warning(userId:Long, firstFailTime:Long, lastFailTime:Long, warningMsg:String)


object LoginFail {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    //读取事件数据
    val rescource = getClass.getResource("/LoginLog.csv")
    val loginEventStream = env.readTextFile(rescource.getPath)
      .map( data =>{
        val dataArray = data.split(",")
        LoginEvent(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(5)) {
        override def extractTimestamp(element: LoginEvent): Long = element.eventTime * 1000L
      })

    val warnningStream = loginEventStream
      .keyBy(_.userId)
      .process( new LoginWarning(2))

    warnningStream.print()
    env.execute("login fail detect job")



}
  class LoginWarning(maxFailTime: Int) extends KeyedProcessFunction[Long, LoginEvent, Warning] {
    //定义状态保存两秒内登陆失败事件
    lazy val loginFailState:ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("login-fail-state",classOf[LoginEvent]))

    override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#Context, out: Collector[Warning]): Unit = {
/*      val loginFailList = loginFailState.get()
      //判断类型是否是fail，是的话添加fail的事件到fail
      if (value.eventType == "fail"){
        if (!loginFailList.iterator().hasNext){
          ctx.timerService().registerEventTimeTimer(value.eventTime * 1000L +2000L)
        }
        loginFailState.add(value)
      }else{
        //如果成功 清空状态
        loginFailState.clear()
      }*/
      if (value.eventType == "fail"){
        //如果失败，判断之前是否有登陆失败的事件
        val  iter = loginFailState.get().iterator()
        if (iter.hasNext){
          //如果有登陆失败事件比对事件
          val firstFail = iter.next()
          if(value.eventTime < firstFail.eventTime + 2){
            //如果两次间隔小于两秒 输出报警
            out.collect(Warning(value.userId,firstFail.eventTime,value.eventTime,"login fail in 2 seconds"))
          }
          //更新最近的一次登陆失败事件 保存在状态里
          loginFailState.clear()
          loginFailState.add(value)
        }else{
          //如果第一次登陆失败直接添加状态
          loginFailState.add(value)
        }

      }else{
        //如果成功，清空状态
        loginFailState.clear()
      }
    }


    }

/*    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#OnTimerContext, out: Collector[Warning]): Unit = {
      //触发定时器的时候，根据状态里的个数决定是否输出报警
      val allLoginFails:ListBuffer[LoginEvent] = new ListBuffer[LoginEvent]
      val iter = loginFailState.get().iterator()
      while (iter.hasNext){
        allLoginFails += iter.next()
      }
      //判断个数
      if (allLoginFails.length >= maxFailTime){
        out.collect(Warning(allLoginFails.head.userId, allLoginFails.head.eventTime, allLoginFails.last.eventTime,"login fail in 2 seconds for" + allLoginFails.length+1))
      }
      //清空状态
      loginFailState.clear()
    }
  }*/
}
