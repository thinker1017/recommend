package homed.ilogslave.dataWarehouse.userBehavior
import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import homed.Options
import homed.config.ReConfig._
import homed.tools._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.rdd.JdbcRDD

import scala.collection.mutable.ListBuffer
import homed.ilogslave.dataWarehouse._

case class user_Video_Hit(
  DA:Long,
  hitTime:Long,
  series:video_serie,
  watchTime:Int,
  watchCount:Int
)

// 热门点击
case class hot_live_Hit(
  series_name:String, 
  actor:String,
  director:String,
  f_tab:String,
  watchCount:Int,
  playTime:Long,
  duration:Int
)




//用户点击 包括直播点击，点播点击
object UserHit {
   
   private val watchEffectiveTime = 3*60*1000
   private val SaveNameMap = Map("liveHit"->"userLiveHit") 
   private var LOGMAXDAY = 30
   private val timePattern = """.*(\d\d):(\d\d):(\d\d):.*""".r
   
   private val enterPattern = 
   """\[([0-9]+)\]([0-9-:\s]+) - \[INFO\] - ProgramEnter,DA ([0-9]+),ProgramID ([0-9]+),SeriesID ([0-9]+)""".r
   
   private val exitPattern = 
   """\[([0-9]+)\]([0-9-:\s]+) - \[INFO\] - ProgramExit,DA ([0-9]+),ProgramID ([0-9]+),SeriesID ([0-9]+)""".r
   private var userLiveHitMap = scala.collection.Map[Long, List[event_serie]]()
   private var LiveHitList = List[user_Video_Hit]()
   private var saveingFilename = ""
   private var savedFilename = ""
   private val livefs = FileSystem.get(new Configuration())
   private val timeAttenuation = 0.5
   private var gNowTime = Calendar.getInstance.getTimeInMillis
   private val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
   
   private var serviceMap = scala.collection.Map[Long,List[homed_eit_schedule_history]]()
   private var serviceEventMap = scala.collection.Map[Long,homed_eit_schedule_history]()
   //series_id与eventSeries 
   private var eventSeriesMap = scala.collection.Map[Long,event_serie]()
   //series_name与eventSeries
   private var eventSeriesNameMap = scala.collection.Map[String,event_serie]()
   //video 节目点播
   private var videoSeriesMap = scala.collection.Map[Long,video_serie]()
   private var videoInfosMap = scala.collection.Map[Long,video_info]()
   
   private var eitSchedulehToSeries = scala.collection.Map[homed_eit_schedule_history,event_serie]()
   
   private var hotSeriesByWc = List[event_serie]()
   private var hotSeriesByPT = List[event_serie]()
   private var userVideoHitList = ListBuffer[user_Video_Hit]()
   
   
   //event_serie 到video_serie的隐似转换
   private implicit def EventToVideo(eit: event_serie) = video_serie(eit.series_id,eit.series_name,eit.service_id,5,"",
        1,eit.actors,eit.director,"内地","普通话",eit.description,1,"",eit.f_edit_time,eit.f_tab,80,"")
 
   private def getEnterOrExitInfo(line:String): Tuple4[Long, Long, String,String] = {
        line match {
          case enterPattern(pid, time, uid, programId, seriesId) =>{
            (uid.toLong,seriesId.toLong ,time,"S")
          }
          case exitPattern(pid, time, uid, programId, seriesId) =>{
            (uid.toLong,seriesId.toLong ,time,"T")
          }
          case _ => {
            (0L, 0L, "0","")
          }
        }
   }
   private def initFilename()={
        saveingFilename = RTool.getSaveFileName("liveHit",false)
        savedFilename = RTool.getSaveFileName("liveHit",true)
        MyLogger.debug(s"savedFilename:${savedFilename},saveingFilename:${saveingFilename}")
   }
   private def initData(sc:SparkContext)={
        //serviceid 对应节目，节目按starttime排序
        serviceMap = sc.makeRDD(Live.geteitScheduleHistory()).map(x=>x.homed_service_id->x).aggregateByKey(ListBuffer[homed_eit_schedule_history]())((u, v) => u+=v, (u1, u2) => u1 ++ u2).mapValues(a=>a.toList.sortWith((s,t)=>(s.start_time.getTime < t.start_time.getTime))).collectAsMap()
        serviceEventMap = sc.makeRDD(Live.geteitScheduleHistory()).map(x=>x.event_id.toLong->x).collectAsMap()
        eventSeriesMap = Live.geteventSeries.map(x=>x.series_id->x).toMap
        eventSeriesNameMap = Live.geteventSeries.map(x=>x.series_name->x).toMap
        eitSchedulehToSeries = Live.geteitSchedulehToSeries()
        videoSeriesMap = videoSeries.getVideoSeries().map(x=>x.series_id->x).toMap
        videoInfosMap = videoSeries.getVideoInfos().map(x=>x.video_id->x).toMap 
        initFilename
   }
   
   private def loadLiveHits(sc:SparkContext)={
        MyLogger.debug("loadLiveHits")
        var backup = ListBuffer[RDD[user_Video_Hit]]()
        try{
          if(livefs.exists(new Path(savedFilename))){
            MyLogger.debug(s"has file savedFilename")
            backup += sc.objectFile[user_Video_Hit](savedFilename)
          }
        }
        catch{
           case e:Exception => {
             e.printStackTrace()
             MyLogger.debug("err"+e)
           }
        }finally {
        }
        sc.union(backup)
   }
   private def saveLiveHits(sc:SparkContext,hitrdd:RDD[user_Video_Hit])={
        MyLogger.debug("saveLiveHits")
        try{
            hitrdd.saveAsObjectFile(saveingFilename)
        }catch{
            case e:Exception => {MyLogger.debug("err"+e)}
            MyLogger.debug(s"saveAsObjectFile $saveingFilename err")
        }
   }
   
   def getLiveHits(sc:SparkContext)={
       initData(sc)
       //testData(sc)
       if(userLiveHitMap.size <= 0 ){
              val backupRDD = loadLiveHits(sc)
              backupRDD.cache()
              
              //有备份只读取2天的
              if(backupRDD.count != 0) LOGMAXDAY = 2
              
              val ymdFormat = new SimpleDateFormat("yyyy-MM-dd")
              val calendar = Calendar.getInstance()
              calendar.add(Calendar.DAY_OF_YEAR, -LOGMAXDAY)
              val service = Options.getServerId()
              val rddhitList = new ListBuffer[RDD[(Long,Long,String,String)]]
              val fs = FileSystem.get(new Configuration())
              
              for (i <- 1 to LOGMAXDAY) {
                val fileName = ilogslaveRunLogPrefix + ymdFormat.format(calendar.getTime()) 
                MyLogger.debug(s"file $fileName")
                try {
                  val pathStatus = fs.listStatus(new Path(fileName));
                  if (pathStatus !=null){
                    for (p <- pathStatus) {
                      rddhitList += sc.textFile(p.getPath.toString).filter(s => s.matches("^.*\\[INFO\\] - Program(Enter|Exit),DA.*")).map(getEnterOrExitInfo)
                    }
                  }
                  else MyLogger.debug(s"no log file $fileName")
                }catch {
                  case e:Exception => {e.printStackTrace}
                }
                calendar.add(Calendar.DAY_OF_YEAR, +1)
              }
              MyLogger.debug("rddhitList init")
              getPlayInfo(sc,sc.union(rddhitList))
              MyLogger.debug("userVideoHitList init,"+userVideoHitList.size)
              val updateVideoHit = sc.makeRDD(userVideoHitList) ++ backupRDD
              val debug = updateVideoHit.map(x=>x.DA->x).combineByKey(v => (new ListBuffer[user_Video_Hit]) += v, (c: ListBuffer[user_Video_Hit], v) => c += v, (c1: ListBuffer[user_Video_Hit], c2: ListBuffer[user_Video_Hit]) => c1 ++ c2)
              debug.collect.foreach(x=>{
                  MyLogger.debug("da:"+x._1)
                  x._2.foreach { x => MyLogger.debug(s"${x.series.series_name},${x.watchTime},${x.watchCount},${x.hitTime}")}
              })

              saveLiveHits(sc, updateVideoHit)
              
              generateHotLiveHit()
              printResult
         }
         userLiveHitMap 
    }
    private def updateUserLiveHit(oldHit:user_Video_Hit)={
         val series_name = oldHit.series.series_name.replaceAll("-revise", "").replaceAll("[(|0-9)]", "").split(":")(0)
         MyLogger.debug(s"before hit ${oldHit.series.series_id},name:${series_name}")
         if(eventSeriesNameMap.contains(series_name)){
           val updateSeries = eventSeriesNameMap.getOrElse(series_name, null)
           val updateHit = user_Video_Hit(oldHit.DA,oldHit.hitTime,updateSeries,oldHit.watchTime,oldHit.watchCount)
           MyLogger.debug(s"update hit info,${updateHit.series.series_id},${updateHit.series.series_name}")
           updateHit
         }else oldHit
       
    }
    private def generateHotLiveHit()={
        MyLogger.debug("HotHitList result:")
        val HotHitList = LiveHitList.map(x=>{
            hot_live_Hit(x.series.series_name,x.series.actors,x.series.director,x.series.f_tab,x.watchCount,x.watchTime,0)
        })
        HotHitList.foreach(x=>MyLogger.debug(x.toString()))
        val hotListByWC = HotHitList.map(x=>x.series_name->x).sortWith((s,t)=>s._2.watchCount > t._2.watchCount)
        val hotListByPT = HotHitList.map(x=>x.series_name->x).sortWith((s,t)=>s._2.playTime > t._2.playTime)
        MyLogger.debug("hotSeriesByWc")
        hotSeriesByWc = hotListByWC.dropRight(hotListByWC.size - 100).map(x=>{
          eventSeriesNameMap.getOrElse(x._1, null)
        }).filter(x=>x != null)
        hotSeriesByWc.foreach(x=>MyLogger.debug(x.toString()))
        MyLogger.debug("hotSeriesByPT")
        hotSeriesByPT = hotListByPT.dropRight(hotListByPT.size - 100).map(x=>{
          eventSeriesNameMap.getOrElse(x._1, null)
        }).filter(x=>x != null)
        hotSeriesByPT.foreach(x=>MyLogger.debug(x.toString()))
    }
    def getHotSeriesByWC()={
        hotSeriesByWc
    }
    
    def getHotSeriesByPT()={
        hotSeriesByPT
    }
    def getUserVideoHitList()={
        userVideoHitList.toArray
    }
    
    private def getSecond(time: String): Long = {
        val timePattern(hours, minuters, seconds) = time
        hours.toInt * 60 * 60 + minuters.toInt * 60 + seconds.toInt
    }
    private def addHitInfo(head:Tuple2[ Long, Long],start:String,etime:String,plays:Long,playCount:Int)={
        val program_id = head._2
        val user_id = head._1
        val hitTime = getSecond(start)
        if(program_id >= PROGRAM_CHANNEL_BEGIN && program_id <= PROGRAM_CHANNEL_END){//直播
              val liveHit = findWatchEvent(user_id,program_id,start,etime)
              liveHit.foreach(x=>MyLogger.debug(s"add live hit:${user_id},${x.series.series_id},${x.series.series_name}"))
              userVideoHitList ++= liveHit
        }
        else if(program_id >= PROGRAM_SERIES_VIDEO_ID_BEGIN && program_id <= PROGRAM_SERIES_VIDEO_ID_END){//点播
              MyLogger.debug(s"vod ${program_id}")
              val series = videoSeriesMap.getOrElse(program_id, null)
              if(series != null) {
                  MyLogger.debug(s"add vod hit ${user_id},${series.series_id},${series.series_name}")
                  userVideoHitList += user_Video_Hit(user_id,hitTime,series,plays.toInt,playCount)
              }
        }
        else if(program_id >= PROGRAM_SERIES_EVENT_ID_BEGIN && program_id <= PROGRAM_SERIES_EVENT_ID_END){//回看
              val series = eventSeriesMap.getOrElse(program_id, null)
              if(series != null) {
                  MyLogger.debug(s"add event hit ${user_id},${series.series_id},${series.series_name}")
                  userVideoHitList += user_Video_Hit(user_id,hitTime,series,plays.toInt,playCount)
              }
        }
    }
    private def getDurations(lineHead: Tuple2[ Long, Long], list : List[Tuple2[String, String]]): Unit = {
      list match {
        case Nil => Unit
        case (start, "S") :: (end, "T") :: tail => {
          val plays = getSecond(end) - getSecond(start) 
          addHitInfo(lineHead,start,end,plays,1)
          getDurations(lineHead, tail)
        }
        case (exit, "T") :: tail => {
          addHitInfo(lineHead,exit,exit,0,1)
          getDurations(lineHead, tail)
        }
        case (start, "S") :: tail => {
          addHitInfo(lineHead,start,start,0,1)
          getDurations(lineHead, tail)
        }
        case other :: tail => getDurations(lineHead, tail)
      }
    }
    /*private def getDurations(lineHead: Tuple2[ Long, Long], list : List[Tuple2[String, String]],result:List[(String, String,Long,Int)]):List[(String, String,Long,Int)] = {
        list match {
          case Nil => result
          case (start, "ProgramEnter") :: (end, "ProgramExit") :: tail => {
            val plays = getSecond(end) - getSecond(start) 
            getDurations(lineHead, tail,result ++ List((start,end,plays,1)))
          }
          case (exit, "ProgramExit") :: tail => {
            getDurations(lineHead, tail,result ++ List((exit,exit,0L,1)))
          }
          case (start, "ProgramEnter") :: tail => {
            getDurations(lineHead, tail,result ++ List((start,start,0L,1)))
          }
          case other :: tail => getDurations(lineHead, tail,result )
        }
    }*/
  
    private def getPlayInfo(sc:SparkContext,rddevent:RDD[(Long, Long, String, String)])={
        // val hitRdd = new PairRDDFunctions(rddevent.map(x=>(x._1,x._2)->(x._3,x._4)))
         //根据 用户id，节目id，作为key，合并，计算用户对节目的观看次数和时长
         
         rddevent.cache()
         MyLogger.debug("rddevent size="+ rddevent.count())
         rddevent.collect.foreach(x=>MyLogger.debug(x.toString()))
         val userListRdd = rddevent.map(x=>(x._1,x._2)->(x._3,x._4)).combineByKey(v => (new ListBuffer[Tuple2[String, String]]) += v, (c: ListBuffer[Tuple2[String, String]], v) => c += v, (c1: ListBuffer[Tuple2[String, String]], c2: ListBuffer[Tuple2[String, String]]) => c1 ++ c2)
         
         userListRdd.collect.foreach(line => {
             //MyLogger.debug("s"+line._2.mkString("|"))
             getDurations(line._1,line._2.sorted.toList)
         })
         MyLogger.debug("getPlayInfo out")
    }
    private def testData(sc:SparkContext)={
         MyLogger.debug(s"this is test data:")
         val testHit1 = scala.collection.Map(50002920L->ListBuffer((4200851411L,"2016-04-25 03:31:00",1),(4200851354L,"2016-04-25 03:58:02",1),(4200851462L,"2016-04-27 12:35:00",1)))
        //val a = getHitEvent(sc,testHit1.toMap)
         val testHit2 = scala.collection.Map(50002920L->ListBuffer((4200851411L,"2016-04-25 03:31:00",1),(4200851411L,"2016-04-25 03:37:00",0),(4200851354L,"2016-04-25 03:58:02",1),(4200851354L,"2016-04-25 04:18:02",1),(4200851462L,"2016-04-27 12:35:00",0),(4200851462L,"2016-04-27 12:35:01",0)))
        //val b = getHitEvent(sc,testHit2.toMap)
         
         //val unionRdd = sc.makeRDD((a ++ b)).map(x=>((x.DA,x.event_Id)->x)).reduceByKey((x, y) => user_Live_Hit(x.DA,x.hitTime,x.event_Id,x.SeriesID,x.watchTime + y.watchTime,x.watchCount)).map(x=>x._2)
         //unionRdd.foreach(x=>MyLogger.debug(x.toString()))
    }
    private def printResult()={
        MyLogger.debug("user event hit:")
        userLiveHitMap.foreach(x=>MyLogger.debug(x.toString()))
    }
    
    private def create_userLiveHit(litem: homed_eit_schedule_history,DA:Long,hitTime:Long,watchTime:Long,watchCount:Int)={
        val e_serie = eitSchedulehToSeries.getOrElse(litem, event_serie(0L,litem.event_name,litem.homed_service_id,"","","","",litem.f_edit_time))
        user_Video_Hit(DA,hitTime,e_serie,watchTime.toInt,watchCount)
    }
    
    private def findWatchEvent(user:Long,serviceid:Long,stime:String,etime:String)={
       var hs = new ListBuffer[(user_Video_Hit)]()
       var weight = 1
       var done = false
       
       if(serviceMap.contains(serviceid)){
          MyLogger.debug(s"watch:$serviceid,user:$user")
          val liveitem = serviceMap.get(serviceid).toList.flatten
          try{
            val _stime = sdf.parse(stime).getTime()
            var _etime = sdf.parse(etime).getTime()
            MyLogger.debug(s"stime=${_stime},e=${_etime}")
            var sInterval  = false
            
            for (litem <- liveitem if !done ){
              val showStime = litem.start_time.getTime
              val showDuration = litem.duration.getTime + 28800000L
              MyLogger.debug(s"${_stime},${_etime},${showStime},${showDuration},${litem.event_name}")
              if(_stime >= showStime  && _stime < (showStime + showDuration )){
                MyLogger.debug(s"find play live show s:$litem")
                //直播退出时间大于该节目结束时间或者结束时间为0
                val watchTime = if(_etime > showStime || _etime == 0L) (showStime + showDuration - _stime) else (_etime - _stime)
                hs += create_userLiveHit(litem,user,_stime,watchTime,weight )
                weight += 1
                sInterval = true
                if(_etime == 0L) done = true
              }else if(sInterval && _etime >= showStime ){//直播退出时间大于节目开始时间，看了该节目
                MyLogger.debug(s"find play live show e:$litem")
                hs += create_userLiveHit(litem,user,_stime,_etime - showStime,weight )
                weight += 1
              }else if(_etime < showStime ){//直播退出时间小于节目开始时间，匹配完成
                done = true
              }else if (_stime < showStime) //过时节目
                done = true
            }
          }
          catch{
             case e:Exception => {
              e.printStackTrace
              MyLogger.debug(s"err in findWatchEvent")
            }
          }
       }
       
       hs.toList
    }

}