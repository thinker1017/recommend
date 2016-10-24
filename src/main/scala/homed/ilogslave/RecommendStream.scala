package homed.ilogslave
import scala.collection.mutable.ListBuffer
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.rdd.JdbcRDD

import java.util.concurrent.{Executors, ExecutorService}
import akka.actor._
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Props
import java.text.SimpleDateFormat
import java.util.Calendar
import java.sql.{ Connection, DriverManager,Date }

import homed.config.ReConfig._
import homed.ilogslave.dataFilter._
import homed.ilogslave.recommendAlgorithm._
import homed.tools._
import homed.tools.MyLogger
import homed.Jdbc._



/*
  val DtvsJDBCUrl = "jdbc:mysql://192.168.35.103:3306/homed_dtvs"
  val DtvsUser = "root"
  val DtvsPasswd = "123456"
 * 
 * */

case class homed_eit_schedule_autocheck (
      homed_network_id:Long,
      homed_service_id:Long,
      event_id:Long,
      event_name:String,
      start_time:java.sql.Timestamp,
      duration:java.sql.Timestamp
)

case class eit_checkList(
      eitlist:Array[homed_eit_schedule_autocheck]      
)

case class eit_resultList(
      relist:Array[t_content_recommend]      
)
object Tools{
    def getTime()={
        val calendar = Calendar.getInstance()
        val ymdFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val today = ymdFormat.format(calendar.getTime)
        today
    }
}



//检测mysql最新数据
class MysqlReadActor(threadName:String,sc:SparkContext) extends Actor{
      val sqlContext = new org.apache.spark.sql.SQLContext(sc)
      private var prop = new java.util.Properties
      private var updatenow = "2016-07-05 09:06:28"
      
     //读取直播epg更新间隔
      private val readLiveEventDuration = 60*1000 
      private def readMysql()={
          //这里循环后recieve是收不到消息了
          while(true){
              MyLogger.info("[MysqlReadActor] read homed_eit_schedule_autoCheck date:"+updatenow)
              val sqljdbc = sqlContext.read.jdbc(DtvsJDBCUrl,"homed_eit_schedule_autoCheck", Array(s"update_time >= '${updatenow}'"),prop)
              val eits = sqljdbc.map(a=>{
                       homed_eit_schedule_autocheck(0L,a.getAs[Long]("homed_service_id"),a.getAs[Int]("event_id"),a.getAs[String]("event_name"),a.getAs[java.sql.Timestamp]("start_time"),a.getAs[java.sql.Timestamp]("duration")) 
              }).collect()
              MyLogger.debug("[MysqlReadActor] size="+eits.size)
              eits.foreach(x=>MyLogger.debug(x.toString()))
              updatenow = Tools.getTime()
              if(eits.size > 0){
                  sender ! eit_checkList(eits)
              }
              Thread.sleep(readLiveEventDuration)
          }
      }
     
      override def preStart()={
          Class.forName("com.mysql.jdbc.Driver")
          prop.setProperty("user", DtvsUser)
          prop.setProperty("password", DtvsPasswd)
          prop.setProperty("zeroDateTimeBehavior", "convertToNull")
          
      }
      def receive = {
          case "read" =>{
              MyLogger.info("[MysqlReadActor] MysqlReadActor read")
              readMysql
          }
         
          case _ => MyLogger.warn("[MysqlReadActor] other msg")
      }
} 

//删除mysql旧数据
class MysqlDeleteActor(threadName:String,sc:SparkContext)  extends Actor {
      val sqlContext = new org.apache.spark.sql.SQLContext(sc)
      private var prop = new java.util.Properties
      private var delnow = Tools.getTime()
      //删除直播epg更新间隔
      private val deleteLiveEventDuration = 10*60*1000
      
      override def preStart()={
          Class.forName("com.mysql.jdbc.Driver")
          prop.setProperty("user", DtvsUser)
          prop.setProperty("password", DtvsPasswd)
          prop.setProperty("zeroDateTimeBehavior", "convertToNull")
          delMysql
      }
      private def delMysql()={
          while(true){
              delnow = Tools.getTime()
              Thread.sleep(deleteLiveEventDuration)
              val connection = DriverManager.getConnection(DtvsJDBCUrl+"?characterEncoding=UTF-8", DtvsUser, DtvsPasswd)
              val statement = connection.createStatement
              val delsql = s"delete from homed_eit_schedule_autoCheck where update_time <= '${delnow}'"
              MyLogger.info(s"[MysqlDeleteActor] delsql = $delsql")
              statement.executeUpdate(delsql)
              MyLogger.info("[MysqlDeleteActor] del homed_eit_schedule_autoCheck:date"+delnow)
          }
      }
      def receive = {
          
          case _ => MyLogger.warn("[MysqlDeleteActor] other msg")
      }
} 


import org.apache.http.client.methods.HttpPost
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.commons.httpclient.methods.PostMethod
import org.apache.http.util.EntityUtils
import org.apache.http.entity.StringEntity
import org.apache.commons.httpclient.HttpStatus
import org.json._

class RecommendNotifySlaveActor(master: ActorRef,threadName:String,sc:SparkContext) extends Actor {
  
      def notifySlave():Boolean={
            val client = new DefaultHttpClient()
            SLAVE_HOST.foreach(slave=>{
                val url = "http://"+slave+":13160/recommend/schedule"
                val post = new HttpPost(url)
                val jsonObject = new JSONObject()
                jsonObject.put("type", "5")
                jsonObject.put("value", "5")
                val s = new StringEntity(jsonObject.toString())
                s.setContentEncoding("UTF-8")
                s.setContentType("application/json")
                post.setEntity(s)
                val res = client.execute(post)
                if(res.getStatusLine().getStatusCode() == HttpStatus.SC_OK){
                    val entity = res.getEntity()
                    val result = EntityUtils.toString(res.getEntity())
                    
                    if(result.contains("success")){
                        MyLogger.info("[RecommendNotifySlaveActor] success notify:"+slave)
                        return true
                    }
                  
                }else{
                    MyLogger.info("[RecommendNotifySlaveActor] err notify:"+slave)
                }
            })
            return false
      }
     
      def receive = {
          case "notify" => {
              MyLogger.info("[RecommendNotifySlave] recieved notify")
              val _re = notifySlave
              if(_re)
                  master ! "notifySuccess"
              else
                  master ! "notifyFailed"
          }
          case _ => MyLogger.warn("[RecommendNotifySlave] other msg")
      }
}
//发送更新，删除旧数据
class RecommendResultActor(threadName:String,sc:SparkContext) extends Serializable with Actor{
  
     def saveContentRecommend(arr:Array[t_content_recommend])={
          MyLogger.debug("[RecommendResultActor] save ContentRecommend to mysql")
          val connection = DriverManager.getConnection(IlogJDBCUrl+"?characterEncoding=UTF-8", IlogUser, IlogPasswd)
          val statement = connection.createStatement
          val preStatement = connection.prepareStatement("insert into t_content_recommend (f_program_id,f_tag_name,f_index,f_recommend_id,f_play_time,f_recommend_reason,f_recommend_date) values (?,?,?,?,?,?,?)")
          var updateSize = 0
          arr.foreach(x=>{
              preStatement.setLong(1, x.f_program_id)
              preStatement.setString(2, x.f_tag_name)
              preStatement.setLong(3, x.f_index)
              preStatement.setLong(4, x.f_recommend_id)
              preStatement.setTimestamp(5, x.f_play_time)
              preStatement.setString(6, x.f_recommend_reason)
              preStatement.setTimestamp(7, x.f_recommend_date)
              try{
                  preStatement.executeUpdate()
                  updateSize += 1
              }
              catch{
                 case e:Exception => {
                   e.printStackTrace()
                   MyLogger.error("[RecommendResultActor] err"+e)
                   MyLogger.error(s"[RecommendResultActor] save to db err ${x}")
                 }
              }
          })
          updateSize
     }
      //删除要更新的id
     def DeleteOldDataByDate(date:String)={
         val connection = DriverManager.getConnection(IlogJDBCUrl+"?characterEncoding=UTF-8", IlogUser, IlogPasswd)
         val statement = connection.createStatement
        
				 val preStatement = connection.prepareStatement(s"delete from t_content_recommend where f_recommend_date <=$date")
				 try{
                  preStatement.executeUpdate()
              }
              catch{
                 case e:Exception => {
                   e.printStackTrace()
                   MyLogger.error("[RecommendResultActor] err"+e)
                   MyLogger.error("[RecommendResultActor] deleteByDate err ")
              }
         }
     }
     //删除要更新的id
     def DeleteOldDataById(ids:Array[Long])={
         val connection = DriverManager.getConnection(IlogJDBCUrl+"?characterEncoding=UTF-8", IlogUser, IlogPasswd)
         val statement = connection.createStatement
         var sql="delete from t_content_recommend where f_program_id in(0"
         ids.foreach(id=>{ 
            sql += ","+id 
         })
				 sql+=")"
				 MyLogger.info("[RecommendResultActor] delete sql:"+sql)
				 val preStatement = connection.prepareStatement(sql)
				 try{
              preStatement.executeUpdate()
         }
         catch{
              case e:Exception => {
                   e.printStackTrace()
                   MyLogger.error("[RecommendResultActor] err"+e)
                   MyLogger.error("[RecommendResultActor] deleteById err ")
              }
         }
     }
     //删除更新时间过久的数据
     def updateRecommend(recommend:eit_resultList)={
         val result = recommend.relist
         val updateProgarmList = result.map(_.f_program_id).distinct
         DeleteOldDataById(updateProgarmList)
         //分区保存到mysql，提高保持效率
         val resultRDD = sc.makeRDD(result)
         //var saveTotal = 0
         val saveTotal = sc.accumulator(0, "saveNum Accumulator")
         val nowTime = System.currentTimeMillis()
         MyLogger.info("[RecommendResultActor] resultRDD partitions:"+resultRDD.partitions.size)
          //单机模式也会8个分区同时保存
         resultRDD.foreachPartition { presult => {
             //saveContentRecommend(presult.toArray)
             val arr = presult.toArray
             if(arr.size > 0){
                  MyLogger.debug("[RecommendResultActor] save ContentRecommend to mysql")
                  val connection = DriverManager.getConnection(IlogJDBCUrl+"?characterEncoding=UTF-8", IlogUser, IlogPasswd)
                  val statement = connection.createStatement
                  val preStatement = connection.prepareStatement("insert into t_content_recommend (f_program_id,f_tag_name,f_index,f_recommend_id,f_play_time,f_recommend_reason,f_recommend_date) values (?,?,?,?,?,?,?)")
                  var updateSize = 0
                  arr.foreach(x=>{
                      preStatement.setLong(1, x.f_program_id)
                      preStatement.setString(2, x.f_tag_name)
                      preStatement.setLong(3, x.f_index)
                      preStatement.setLong(4, x.f_recommend_id)
                      preStatement.setTimestamp(5, x.f_play_time)
                      preStatement.setString(6, x.f_recommend_reason)
                      preStatement.setTimestamp(7, x.f_recommend_date)
                      try{
                          preStatement.executeUpdate()
                          updateSize += 1
                      }
                      catch{
                         case e:Exception => {
                           e.printStackTrace()
                           MyLogger.error("[RecommendResultActor] err"+e)
                           MyLogger.error(s"[RecommendResultActor] save to db err ${x}")
                         }
                      }
                  })
                  MyLogger.debug(s"[RecommendResultActor] save total:${arr.size},success:${updateSize}")
                  saveTotal += updateSize
               }
         } }
         val saveSec = (System.currentTimeMillis() - nowTime)/1000.0
         MyLogger.debug(s"[RecommendResultActor] savetoMysql run:"+saveSec+"sec")
         if(saveSec > 60.0){
             MyLogger.warn(s"[RecommendResultActor] savetoMysql cost more than 60s")
         }
         saveTotal
         //saveContentRecommend(result)
     }
     def receive = {
          case recommend:eit_resultList => {
              MyLogger.info("[RecommendResultActor] recieved recommend"+recommend.relist.size)
              val sendsuccesscnt = updateRecommend(recommend).value
              MyLogger.info("[RecommendResultActor] total:"+recommend.relist.size+",savetomysql success:" + sendsuccesscnt)
              if(recommend.relist.size == sendsuccesscnt)
                  sender ! "saveSuccess"
              else  
                  sender ! "saveFailed"
          }
          case "cron" =>{//删除旧数据
              val calendar = Calendar.getInstance()
              val ymdFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
              calendar.add(Calendar.HOUR, -12)
              val crondate = ymdFormat.format(calendar.getTime)
              MyLogger.info("[RecommendResultActor] cron:"+crondate)
              DeleteOldDataByDate(crondate)
          }
          case _ => MyLogger.warn("[RecommendResultActor] other msg")
      }
} 

import homed.ilogslave.dataWarehouse._

class RecommendCalcActor(threadName:String,sc:SparkContext) extends serializable with Actor {
     private var simMap = Map[String, List[(String, Double)]]()
     private var series = Array[event_serie]()
     private var eitSchedules = ListBuffer[homed_eit_schedule]()
     private var seriesformatNameMap = Map[String, event_serie]()
     private var eitFormatName = ListBuffer[String]()
     private var eitFormatNameEventMap = scala.collection.Map[String, List[homed_eit_schedule]]()
     private var eitIdMap = scala.collection.Map[Int, homed_eit_schedule]()
     private var seriesIdMap = scala.collection.Map[Long, event_serie]()
     private var showIndex = Map[Long,Int]()
     private var channelInfo = Map[Long, String]()
     private var eitchannelMap = Map[Int, String]()
     private var eitIndexMap = scala.collection.Map[Int,homed_eit_schedule]()
     private var eventIdNameMap = Map[Int, (String,Long,Long,Long)]()
     private var seriesIdNameMap = Map[Long, (String,Long,Long,Long)]()
     private val advancedEitList = ListBuffer[homed_eit_schedule_autocheck]()
    
     implicit def eitToEith(x: homed_eit_schedule_autocheck ) = homed_eit_schedule(x.homed_service_id,x.event_id.toInt,x.event_name,x.start_time,x.duration)
     implicit def eitaToEit(x: homed_eit_schedule ) = homed_eit_schedule_autocheck(0L,x.homed_service_id,x.event_id.toInt,x.event_name,x.start_time,x.duration)
     
     implicit def longToInt(x: Long ) = x.toInt
     //初始化剧集和直播数据
     def InitEventAndSeries()={
         
         Live.Init(sc)
         columnInfo.Init(sc)
         DataFilter.Init(sc)
         eitSchedules.clear()
         eitSchedules ++= Live.geteitSchedule()
         series = Live.geteventSeries()
         MyLogger.info("[RecommendCalcActor] eit:"+eitSchedules.size+",series:"+series.size)
         //辅助数据
         seriesformatNameMap  = series.map(x=>RTool.formatShowName(x.series_name)->x).toMap
         eitFormatName = eitSchedules.map(x=>RTool.formatShowName(x.event_name))
         
         //直播需要考虑重名情况
         eitFormatNameEventMap = sc.makeRDD(eitSchedules).map(x=>RTool.formatShowName(x.event_name)->x).aggregateByKey(ListBuffer[(homed_eit_schedule)]())((u, v) => u+=v, (u1, u2) => u1 ++ u2).mapValues(a=>a.toList).collectAsMap().toMap
         
         channelInfo = Live.getChannelInfo.map(x=>x.channel_id->x.chinese_name).toMap
         eitchannelMap = eitSchedules.map(x=>x.event_id->channelInfo.getOrElse(x.homed_service_id,"")).toMap
         
         //直播节目对应的正在直播的其他节目
         eitIdMap = eitSchedules.map(x=>x.event_id->x).toMap
         seriesIdMap = series.map(x=>x.series_id->x).toMap 
         
         seriesIdNameMap = series.map(x=>x.series_id->(x.series_name,x.series_id,0L,0L)).toMap
         
         eventIdNameMap = eitSchedules.map(x=>x.event_id->(x.event_name,x.event_id.toLong,x.start_time.getTime,x.duration.getTime)).toMap

         //初始化推荐结果的index
         showIndex = Map()
     }
     //初始化相似矩阵，隔断时间更新一次
     def InitSimData()={
          MyLogger.debug("[RecommendCalcActor] InitSimData")
          simMap = CB.ContentBased_itemsSimBroad(sc).toMap
     }
     private def getshowIndex(id:Long)={
          val a = showIndex.getOrElse(id, 0)
          showIndex += ((id,a+1))
          a
     }
     private def initShowIndexById(id:Long)={
          showIndex += ((id,0))
     }
       //2个直播节目播放时间区间有重合列入推荐列表
     private def validLivingShow(s:homed_eit_schedule,t:homed_eit_schedule):Boolean={
          val t1 = t.start_time.getTime
          val t2 = t1 + t.duration.getTime + 28800000L
          val s1 = s.start_time.getTime
          val s2 = s1 + s.duration.getTime + 28800000L
          //MyLogger.debug(Math.min(s2,t2)+","+ Math.max(s1,t1))
          val overlap = Math.min(s2,t2) - Math.max(s1,t1)
          return overlap > 0L && s!=t
     }
     private def filterSameProgram(arr:Array[t_content_recommend])={
          
          val recomEvent = arr.map(x=>{
              if(x.f_recommend_id > PROGRAM_SERIES_EVENT_ID_BEGIN && x.f_recommend_id < PROGRAM_SERIES_EVENT_ID_END){
                  seriesIdNameMap.getOrElse(x.f_recommend_id.toInt, null)
              }else{
                  eventIdNameMap.getOrElse(x.f_recommend_id.toInt, null)
              }
          }).filter(_ != null)
          val recomFormat = recomEvent.map(x=>(RTool.formatShowName(x._1) ,x._2,x._3,x._3 + x._4)).sortWith((s,t)=>s._3 > t._3)
          val result = recomFormat.groupBy(_._1)
          
          val filtered = result.flatMap(li =>{
              //回看
              val event = li._2.filter(_._3 == 0L).take(1)
              val live = li._2.filter(_._3 != 0L).groupBy(x=>(x._3/60000.0 + 0.5).toInt).flatMap(same=>same._2.take(1)).toArray
              event ++ live
          }).map(_._2).toList
          MyLogger.debug("[RecommendCalcActor]"+arr.size+",filtered = "+filtered.size+",recomEvent="+recomEvent.size)
          val filterList = arr.filter(x=>filtered.contains(x.f_recommend_id))
          val filteredList = arr.toSet &~ filterList.toSet
          MyLogger.debug(s"[RecommendCalcActor] filterdsame :")
          filteredList.foreach(x=>MyLogger.debug("filter:"+x))
          filterList
     }
     def needUpdate(one:homed_eit_schedule_autocheck):Boolean={
          val oldeit = eitIdMap.getOrElse(one.event_id,null)
          //信息没有变化则不更新
          if(oldeit != null && one.event_name == oldeit.event_name && one.homed_service_id == oldeit.homed_service_id
              && one.start_time == oldeit.start_time && one.duration == oldeit.duration)
              return false
          return true
     }
     def eventUpdate(live:Array[homed_eit_schedule_autocheck])={
          val updateId = live.map(_.event_id)
          var (updatecnt,samecnt,nocnt) = (0,0,0)
          //删除旧数据
          val calendar = Calendar.getInstance()
          val ymdFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          val needUpdateEvent = ListBuffer[Long]()
          MyLogger.info("[RecommendCalcActor] updatetime:"+ymdFormat.format(calendar.getTime))
          calendar.add(Calendar.HOUR, -6)
          val interval_1 = calendar.getTime
          calendar.add(Calendar.HOUR, +12)
          val interval_2 = calendar.getTime
          //删除直播列表过期和超前的数据，当前时间正负6小时都在计算范围
          MyLogger.info("[RecommendCalcActor] interval:["+ymdFormat.format(interval_1)+","+ymdFormat.format(interval_2)+"],eitsize:"+eitSchedules.size)
          eitSchedules = eitSchedules.dropWhile { x => (x.start_time.getTime < interval_1.getTime || x.start_time.getTime > interval_2.getTime || updateId.contains(x.event_id))}
          MyLogger.debug("[RecommendCalcActor] now size"+eitSchedules.size)
          live.foreach(lt=>{
              //是更新event的数据
              if(eitIdMap.contains(lt.event_id)){
                  if(needUpdate(lt)){
                      MyLogger.debug("[RecommendCalcActor] update event:"+eitIdMap.get(lt.event_id))
                      updatecnt += 1
                      needUpdateEvent += lt.event_id
                      eitSchedules += lt
                      eitIdMap += (lt.event_id.toInt -> lt)
                  }
                  else{
                      MyLogger.debug(s"[RecommendCalcActor] $lt is same ,need not update")
                      samecnt += 1
                      eitSchedules += lt
                      eitIdMap += (lt.event_id.toInt -> lt)
                  }
              }//超前epg暂时不计算，只计算未来6小时的epg数据
              /*else if(lt.start_time.getTime > interval_2.getTime ){
                  MyLogger.debug(s"[RecommendCalcActor] $lt is too advance,not update"+lt)
                  advancedEitList += lt
                  MyLogger.debug("[RecommendCalcActor] advancedEitList size:"+advancedEitList.size)
              }*/
              else{
                  nocnt += 1
                  needUpdateEvent += lt.event_id
                  eitSchedules += lt
                  eitIdMap += (lt.event_id.toInt -> lt)
                  MyLogger.debug("[RecommendCalcActor] insert eit info"+lt)
              }
              
          })
          MyLogger.info("[RecommendCalcActor] samecnt:"+samecnt+",updatecnt:"+updatecnt+",nocnt:"+nocnt+",eitSchedules:"+eitSchedules.size)
          eitFormatNameEventMap = sc.makeRDD(eitSchedules).map(x=>RTool.formatShowName(x.event_name)->x).aggregateByKey(ListBuffer[(homed_eit_schedule)]())((u, v) => u+=v, (u1, u2) => u1 ++ u2).mapValues(a=>a.toList).collectAsMap().toMap
          eventIdNameMap = eitSchedules.map(x=>x.event_id->(x.event_name,x.event_id.toLong,x.start_time.getTime,x.duration.getTime)).toMap

          needUpdateEvent
     }
     private def addRandom(eit:(Long,String),tsize:Int)={
         val recommendList = ListBuffer[t_content_recommend]()
         val date = new java.sql.Date(new java.util.Date().getTime)
         val halfsize = tsize/2
         for(i <- 0 to halfsize){
              val num1 = scala.util.Random.nextInt(series.size)
              recommendList += t_content_recommend(eit._1,"",0,series(num1).series_id,series(num1).f_edit_time,series(num1).series_name,new java.sql.Timestamp(date.getTime))
              val num2 = scala.util.Random.nextInt(eitSchedules.size)
              recommendList += t_content_recommend(eit._1,"",0,eitSchedules(num2).event_id,eitSchedules(num2).start_time,eitSchedules(num2).event_name,new java.sql.Timestamp(date.getTime))
         }
         // MyLogger.debug("[RecommendCalcActor] live With NO sim:"+eit._2+","+RTool.formatShowName(eit._2))
         recommendList.foreach(x=>MyLogger.debug("[RecommendCalcActor] "+x.toString()))
         recommendList.toArray
     }
     def calcFilter(live:Array[homed_eit_schedule_autocheck])={
         val need = eventUpdate(live)
         live.filter(x=>need.contains(x.event_id))
     }
     def calcRecommend(live:Array[homed_eit_schedule_autocheck]):Array[t_content_recommend]={
         
         var result = Array[t_content_recommend]()
         if(live.size <= 0){
             return result
         }
         live.foreach(x=>MyLogger.debug("calcRecommend live:"+x.event_id+","+x.event_name))
         val formatShowNameList = live.map(x=>(RTool.formatShowName(x.event_name),x.event_id)).distinct
         
         val validlivingEitList = live.map(s=>s.event_id->eitSchedules.filter(validLivingShow(s,_)).map(x=>x.event_id)).toMap
         val date = new java.sql.Date(new java.util.Date().getTime)
         val stime = System.currentTimeMillis()
         
         
         def addSimLiveAndSeries(show:(String,Long), simShow: List[(String, Double)])={
              val liveEitByTime = validlivingEitList.getOrElse(show._2, null)
              MyLogger.debug("[RecommendCalcActor] simShow")
              simShow.foreach(x=>MyLogger.debug("[RecommendCalcActor] "+x.toString()))
              /*MyLogger.debug("liveEitByTime")
              liveEitByTime.foreach(x=>MyLogger.debug(x))*/
              val liveSimList = simShow.filter(x=>eitFormatNameEventMap.contains(x._1)).map(x=>eitFormatNameEventMap.getOrElse(x._1,null).map((_,x._2 * LiveWeight))).flatten.filter(x=>liveEitByTime.contains(x._1.event_id))
              liveSimList.foreach(x=>MyLogger.debug("[RecommendCalcActor] liv:"+x._1.event_name+","+x._2))
              val seriesSimList = simShow.filter(x=>seriesformatNameMap.contains(x._1)).map(x=>(seriesformatNameMap.getOrElse(x._1,null),x._2 * EventWeight)).filter(_._1 != null)
              seriesSimList.foreach(x=>MyLogger.debug("[RecommendCalcActor] ser:"+x._1.series_name+","+x._2))
                     
              val t2 = liveSimList.take(200).map(x=>(t_content_recommend(show._2,"",0,x._1.event_id,x._1.start_time,x._1.event_name,new java.sql.Timestamp(date.getTime)),RTool.formatShowName(x._1.event_name),x._2))
              val t1 = seriesSimList.filter(x=>DataFilter.validSeries(x._1.series_id)).take(200).map(x=>(t_content_recommend(show._2,"",0,x._1.series_id,x._1.f_edit_time,x._1.f_tab,new java.sql.Timestamp(date.getTime)),x._1.series_name,x._2))
              val liveShow = t2.map(_._2)
              MyLogger.debug("[RecommendCalcActor] seriesSimList:"+seriesSimList.size+":t1:"+t1.size)
                     //去掉相同节目的推荐，优先选择live
              val filtered = t1.filter(t=> !liveShow.contains(RTool.formatShowName(t._2))).map(x=>(x._1,x._3))
              (t2.map(x=>(x._1,x._3)) ++ filtered).sortWith((s,t)=>s._2 > t._2).map(_._1).distinct.toArray
         }
         
         def liveWithNoSim(show:(String,Long))={
              var f_result = Array[t_content_recommend]()
              val fshow = RTool.formatShowName(show._1)
              val simList = CB.ContentBased_itemsSimOne(fshow)
              if(simList.size > 0){
                   //add sim result to map
                   simMap += (fshow->simList.toList)
                   f_result = addSimLiveAndSeries(show,simList.toList)
              }
              else{
                   MyLogger.warn("[RecommendCalcActor] ContentBased_itemsSimOne is empty")
              }
              if(f_result.size < 200){
                   MyLogger.warn("[RecommendCalcActor] warning recommend list is small"+f_result.size)
                   f_result ++= addRandom(show.swap,200 - f_result.size)
                   MyLogger.warn("[RecommendCalcActor] supplement size"+f_result.size)
              }
              f_result
              
         }

         try{
              result = formatShowNameList.flatMap(show=>{
                 val formatShow = RTool.formatShowName(show._1)
                 MyLogger.debug(s"[RecommendCalcActor] $show formatShow=$formatShow")
                 var f_result = Array[t_content_recommend]()
                 initShowIndexById(show._2)
                 if(simMap.contains(formatShow)){
                     
                     //val simList = simMap.getOrElse(formatShow,null)
                     val simShow = simMap.getOrElse(RTool.formatShowName(show._1), null).sortWith((s,t)=>s._2 > t._2).take(500).distinct
                     f_result = addSimLiveAndSeries(show,simShow)
                     MyLogger.debug("#####################")
                     if(f_result.size < 200){
                          MyLogger.warn("[RecommendCalcActor] warning recommend list is small"+f_result.size)
                          f_result ++= addRandom(show.swap,200 - f_result.size)
                          MyLogger.warn("[RecommendCalcActor] supplement size"+f_result.size)
                     }
                     f_result.foreach(x=>
                        if(x.f_recommend_id > PROGRAM_SERIES_EVENT_ID_BEGIN && x.f_recommend_id < PROGRAM_SERIES_EVENT_ID_END){
                            val t = seriesIdMap.getOrElse(x.f_recommend_id,null)
                            if(t != null)
                              MyLogger.debug(s"[RecommendCalcActor] debug series:${x.f_recommend_id},${t.series_name},${t.series_id}")
                        }else{
                            val t = eitIdMap.getOrElse(x.f_recommend_id.toInt,null)
                            if(t != null)
                              MyLogger.debug(s"[RecommendCalcActor] debug living:${x.f_recommend_id},${t.event_name},${t.event_id}")
                     })
                    
                 }else{
                     MyLogger.warn("[RecommendCalcActor] warning: no sim with"+show)
                     f_result = liveWithNoSim(show)
                 }
                 val re = f_result.take(200).map(x=>t_content_recommend(x.f_program_id,x.f_tag_name,getshowIndex(x.f_program_id),x.f_recommend_id,x.f_recommend_date,x.f_recommend_reason,x.f_recommend_date))
                 val filtered = filterSameProgram(re)
                 MyLogger.debug(s"[RecommendCalcActor] $show filtered:"+re.size+","+filtered.size)
                 filtered
             })
         }catch{
              case e:Exception => {
                   e.printStackTrace()
                   MyLogger.error("[RecommendCalcActor] err:"+e)
               }
         }
         MyLogger.info(s"[RecommendCalcActor] calcRecommend end.. run:"+(System.currentTimeMillis() - stime)/1000.0+"seconds")
         //result.foreach(x=>MyLogger.debug(x.toString()))
         
         result
     }
     private def debugRecommend(t_contentRecommend:Array[t_content_recommend])={
       
         val debug = sc.makeRDD(t_contentRecommend).map(x=>x.f_program_id->x).aggregateByKey(ListBuffer[(t_content_recommend)]())((u, v) => u+=v, (u1, u2) => u1 ++ u2).mapValues(a=>a.toList).collectAsMap()
         debug.foreach(tt=>{
            val eit = eitIdMap.getOrElse(tt._1.toInt,null)
            if(eit != null){
                MyLogger.debug("[RecommendCalcActor] eit:"+eit.event_name+","+eit.event_id)
            }
            else{
                MyLogger.warn("[RecommendCalcActor] warning no eit"+tt._1.toString())
            }
            tt._2.foreach(x=>
              if(x.f_recommend_id > PROGRAM_SERIES_EVENT_ID_BEGIN && x.f_recommend_id < PROGRAM_SERIES_EVENT_ID_END){
                  val t = seriesIdMap.getOrElse(x.f_recommend_id,null)
                  if(t == null){
                      MyLogger.warn(s"[RecommendCalcActor] null series:$x")
                  }
                  else{
                      MyLogger.debug(s"[RecommendCalcActor] re series:${x.f_recommend_id},${t.series_name},${channelInfo.getOrElse(t.service_id,"")}")
                  }
              }else{
                  val t = eitIdMap.getOrElse(x.f_recommend_id.toInt,null)
                  if(t == null){
                      MyLogger.warn("[RecommendCalcActor] null event:"+x)
                  }
                  else{
                      MyLogger.debug(s"[RecommendCalcActor] re living:${x.f_recommend_id},${t.event_name},${eitchannelMap.getOrElse(t.event_id,"")}")
                  }
              }
            )
        })
     }
     
     def receive = {
          case "init" =>{
              MyLogger.info("[RecommendCalcActor] recieve init")
              InitEventAndSeries
              InitSimData
              val result = calcRecommend(eitSchedules.toArray.map(x=>eitaToEit(x)))
              sender ! eit_resultList(result)
              debugRecommend(result)
          }
          case live:eit_checkList =>{
              MyLogger.info("[RecommendCalcActor] recieve new event update")
              val result = calcRecommend(calcFilter(live.eitlist))
              sender ! eit_resultList(result)
              debugRecommend(result)
          }
          case "start" =>{
              MyLogger.info("[RecommendCalcActor] recieve start")
          }
          
          case _ => MyLogger.warn("[RecommendCalcActor] other msg")
     }
} 
import java.util.TimerTask
import java.util.Timer
class NotifySchdedule(master: ActorRef) extends TimerTask{
    @Override
    def run() {
        MyLogger.info("[NotifySchdedule] run..,send notify")
        master ! "notify"
    }

}

class RecommendActor(listener: ActorRef,sc:SparkContext)  extends Actor{
      
      private var RecommendStop = false
      private val MysqlReadActor = context.actorOf(Props( new MysqlReadActor("ThreadMysqlRead",sc)))
      private val MysqlDelActor = context.actorOf(Props( new MysqlDeleteActor("MysqlDelActor",sc)))
      private val RecommendCalcActor = context.actorOf(Props( new RecommendCalcActor("RecommendCalcActor",sc)))
      private val RecommendResultActor = context.actorOf(Props( new RecommendResultActor("RecommendResultActor",sc)))
      private val RecommendNotifySlaveActor = context.actorOf(Props( new RecommendNotifySlaveActor(this.self,"RecommendNotifySlaveActor",sc)))
      private val eitStartTime = ListBuffer[Long]()
      private val ymdFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      private var smallStartTime = Long.MaxValue
      private var timer =  new Timer()
      
      
      private def timerNotify():Boolean={
          if(eitStartTime.size <= 0){
              MyLogger.warn("[RecommendActor] eitStartTime is empty,need not notify")
              return false
          }
          val _smallStartTime = eitStartTime.sortWith((s,t)=>s < t)(0) 
          val nowTime = System.currentTimeMillis()
          MyLogger.debug("[RecommendActor] nowTime:"+ymdFormat.format(new Date(nowTime)))
          MyLogger.debug("[RecommendActor] upateTime:"+ymdFormat.format(new Date(_smallStartTime))+",smallupdateTime:"++ymdFormat.format(new Date(smallStartTime)))
          //更新定时器
          if(_smallStartTime <= smallStartTime){
              timer.cancel()
              smallStartTime = _smallStartTime
              MyLogger.debug(s"[RecommendActor] has one more small startTime:$smallStartTime,time cancel")
          }
          val _s = smallStartTime - 60000*10//提前10min
          val notifyTime = if (nowTime > _s) 1 else (_s - nowTime  )
          if(notifyTime < 5*60000){
              MyLogger.warn("notify is less than 5min,warning")
          }
          MyLogger.debug("[RecommendActor] notifyTime:"+notifyTime)
          val notifyTask = new NotifySchdedule(RecommendNotifySlaveActor)
          timer =  new Timer()
          timer.schedule(notifyTask,notifyTime)
          MyLogger.debug(s"[RecommendActor] notify schedule:"+notifyTime)
          
          return true
      }
      def receive = {
          case "start" => {
              MyLogger.info("[RecommendActor] RecommendActor start")
              RecommendCalcActor ! "init"
              MysqlReadActor ! "firstRead"
              //启动后第一次初始化
              eitStartTime += (0L)
              MysqlReadActor ! "read"
              listener ! "RecommendCalcActor init"
              listener ! "MysqlReadActor read"
          }
          //每天定时任务，初始化相似矩阵，删除旧数据
          case "cron" =>{
              RecommendCalcActor ! "init"
              RecommendResultActor ! "cron"
          }
          case "stop" =>{
              MyLogger.info("[RecommendActor] recieve Stop")
              RecommendStop = true
          }
          case "initSim" =>{
              MyLogger.info("[RecommendActor] recieve initSim")
              RecommendCalcActor ! "init"
              listener ! "RecommendCalcActor init"
          }
          case liveArr:eit_checkList =>{
              MyLogger.info("[RecommendActor] received:"+liveArr.eitlist.size) 
              if(liveArr.eitlist.size > 0){
                  RecommendCalcActor ! liveArr
                  listener ! "recieved"+liveArr.eitlist.size+"epg update"
                  //epg播放时间保存
                  eitStartTime ++= liveArr.eitlist.map(x=>x.start_time.getTime)
              }
          }
          case result:eit_resultList =>{
              MyLogger.info("[RecommendActor] recieved recommend result:"+result.relist.size)
              val calendar = Calendar.getInstance()
              val ymdFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
              listener ! "recommend calc finished at:"+ymdFormat.format(calendar.getTime)+",size:"+result.relist.size
              if(result.relist.size >0 ){
                  RecommendResultActor ! result
              }
          }
          case "saveSuccess" =>{
              MyLogger.warn("[RecommendActor] saveSuccess")
              //RecommendNotifySlaveActor ! "notify"
              //设置timer ，不是每次收到有效epg数据都通知slave，而是对比epg的开始时间，在开始时间10分钟前通知slave
              timerNotify
          }
          case "saveFailed" =>{
              MyLogger.warn("[RecommendActor] saveFailed")
          }
          case "notifySuccess" =>{
              MyLogger.info("[RecommendActor] notifySuccess")
              eitStartTime.clear()
              smallStartTime = Long.MaxValue
          }
          case "notifyFailed" =>{
              MyLogger.warn("[RecommendActor] notifyFailed")
          }
          case _ =>{
              MyLogger.warn("[RecommendActor] recieve other msg")
          }
       }
}

class Listener extends Actor {
    def receive = {
      case recomendinfo:String ⇒{
          MyLogger.info("[Listener] :"+recomendinfo)
      }
    }
 }

//实时推荐
object RecommendStream{
       def RecommendStart(sc:SparkContext)={
           try { 
                val system = ActorSystem("RecomendSystem")
                val listener = system.actorOf(Props( new Listener()))
                val RecommendActor = system.actorOf(Props( new RecommendActor(listener,sc)))
                
                RecommendActor ! "start"
                val ymdFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                var iscron = false
                while(true){
                    //30mins
                    Thread.sleep(300000)
                    if(!iscron){
                        val calendar = Calendar.getInstance()
                        if(calendar.getTime.getHours == 23){
                            iscron = true
                            MyLogger.info("[Recommend] need cron at:"+ymdFormat.format(calendar.getTime))
                            RecommendActor ! "cron"
                        }
                    }
                    if(iscron){
                        val calendar = Calendar.getInstance()
                        if(calendar.getTime.getHours == 1)
                            iscron = false
                    }
                }
             
            }finally {
            }
       }

       
}