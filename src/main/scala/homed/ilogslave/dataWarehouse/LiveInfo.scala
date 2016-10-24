package homed.ilogslave.dataWarehouse
import scala.collection.mutable.ListBuffer
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.rdd.JdbcRDD
import homed.config.ReConfig._
import java.text.SimpleDateFormat
import java.util.Calendar
import homed.Options
import java.lang.Math
import homed.Jdbc.showJdbc
import homed.tools._
import homed.config.ReConfig
import homed.Jdbc._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import homed.ilogslave.recommendAlgorithm.Classification
import java.util.regex.Matcher
import java.util.regex.Pattern
//剧集
case class event_serie(
    series_id:Long,
    series_name:String,
    service_id:Long,
    actors:String,
    director:String,
    description:String,
    f_tab:String,
    f_edit_time:java.sql.Timestamp
)
//其他方式获得的直播节目信息
case class liveShow_info(
    f_show_id:Long,
    f_showName:String,
    f_channelName:String,
    f_showInfo:String,
    f_showId:Int,
    f_tab:String,
    f_actors:String,
    f_director:String,
    f_area:String,
    f_releaseYear:String,
    f_date:java.sql.Timestamp
)
//直播节目
case class homed_eit_schedule(
    homed_service_id:Long,
    event_id:Int,
    event_name:String,
    start_time:java.sql.Timestamp,
    duration:java.sql.Timestamp
)
//回看节目
case class homed_eit_schedule_history(
    homed_service_id:Long,
    event_id:Int,
    event_name:String,
    start_time:java.sql.Timestamp,
    duration:java.sql.Timestamp,
    f_edit_time:java.sql.Timestamp,
    f_series_id:Long
)


//直播回看剧集数据
object Live {
    private var eventSeries = Array[event_serie]()
    private var videoDesList = Array[videoDes]()
    private var liveShowInfoMap = Map[Long,liveShow_info]()
    private var SimbySeriesToShowinfo = scala.collection.Map[(Long,String),( Long, String,Double)]()  
    private var eitSchedules = Array[homed_eit_schedule]()
    private var eitScheduleHs = Array[homed_eit_schedule_history]()
    private var ChannelInfo = Array[channelInfo]()
    //eitSchedule与eventseries对应完善节目资料
    private var eitScheduleToSeries = scala.collection.Map[homed_eit_schedule,event_serie]()
    //剧集对应的直播节目list
    private var SeriesToScheduleList = scala.collection.Map[event_serie,List[homed_eit_schedule]]()
    
    //eitSchedulehistory与eventseries对应完善节目资料
    private var eitSchedulehToSeries = scala.collection.Map[homed_eit_schedule_history,event_serie]()
    //eventseries与showinfo合并，补充ttab和description
    private var mergeEventSeries = ListBuffer[event_serie]()
    private val calendar = Calendar.getInstance()
    private val ymdFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    //private var showRelateFromDb = Array[t_show_relate]()
    implicit def eitToEith(arr: Array[homed_eit_schedule]) = arr.map(x=>homed_eit_schedule_history(x.homed_service_id,x.event_id,x.event_name,x.start_time,x.duration,x.duration,1L))
    
    private def StringToDate(d:String)={
        var re  = new java.sql.Timestamp(0l)
       /* try{
          re = new java.sql.Timestamp(ymdFormat.parse(d).getTime)
        }
        catch{
          case e:Exception => {
             e.printStackTrace();
             MyLogger.debug("err"+e+"str:"+d)
           }
        }finally{
        }*/
        re
    }
    def InitChannel(sc:SparkContext)={
         val sqlContext = new org.apache.spark.sql.SQLContext(sc)
         val jdbcModel = new showJdbc("channel_store",DtvsJDBCUrl,DtvsUser,DtvsPasswd)
         ChannelInfo = jdbcModel.getChannelInfo(sc, sqlContext).collect()
    }
    
    //剧集数据补充,下一步将有tagsystem来完成，数据补充，标签化等
    def InitSeries(sc:SparkContext)={
        val sqlContext = new org.apache.spark.sql.SQLContext(sc)
        val jdbcModel = new showJdbc("event_series",DtvsJDBCUrl,DtvsUser,DtvsPasswd)
        val SeriesSRDD = jdbcModel.getEventSeries(sc,sqlContext)
        val HdfsSeriesRDD = SeriesLoadToHdfs(sc)
        
        val needUpdateSeries = SeriesSRDD.map(x=>x.series_name).subtract(HdfsSeriesRDD.map(x=>x.series_name)).collect()
        MyLogger.debug("SeriesSRDD ="+SeriesSRDD.count())
        SeriesSRDD.collect.foreach(x=>MyLogger.debug(x.toString()))
        MyLogger.debug("HdfsSeriesRDD ="+HdfsSeriesRDD.count())
        HdfsSeriesRDD.collect().foreach(x=>MyLogger.debug(x.toString()))
        MyLogger.debug(s"needUpdateSeries size:${needUpdateSeries.size} ")
        //减去hdfs 的数据
        val needUpdateSeriesRDD = SeriesSRDD.filter(x=>needUpdateSeries.contains(x.series_name))
        val videoDesRDD = getVideoDescription(sc)
        
       
        val savedMerge = findShowInfo(sc,videoDesRDD,needUpdateSeriesRDD)
        SeriesSaveToHdfs(sc,sc.makeRDD(savedMerge).union(HdfsSeriesRDD))
        
        MyLogger.debug("mergeEventSeries result:")
        mergeEventSeries.foreach(x=>MyLogger.debug(s"${x.series_name},,${x.actors},,${x.director},,${x.description}"))
        MyLogger.debug("less description series:")
        mergeEventSeries.foreach(x=>{
            if(x.description == null || ( x.description!= null && x.description.length() <= 0 ))
               MyLogger.debug(s"${x.series_name}")
        })
        
    }
    //load 剧集数据
    def SeriesLoadToHdfs(sc:SparkContext)={
        var savedFile = ""
        val file1 = RTool.getSaveFileName("videoDescription",true)
        val file2 = RTool.getSaveFileName("videoDescription",false)
        MyLogger.debug(s"SeriesLoadToHdfs file:$savedFile ")
        val savedRdd = ListBuffer[RDD[event_serie]]()
        try{
          val fs = FileSystem.get(new Configuration())
          if(fs.exists(new Path(file2))) savedFile = file2 else savedFile = file1
          if(fs.exists(new Path(savedFile))){
            MyLogger.debug(s"has file $savedFile")
            savedRdd += sc.textFile(savedFile).map(line=>{
                line.split(":::", 6)
            }).filter(x=>x.length == 6).map(x=>event_serie(0L,x(0),0L,x(1),x(2),x(4),x(3),StringToDate(x(5))))
          }
        }
        catch{
           case e:Exception => {
             e.printStackTrace()
             MyLogger.error("err"+e)
           }
        }finally {
        }
        sc.union(savedRdd)
    }
    //save to hdfs
    private def SeriesSaveToHdfs(sc:SparkContext,mergeRDD:RDD[event_serie])={
        val saveFile = RTool.getSaveFileName("videoDescription", false)
        val seriesRDD = mergeRDD.map(x=>x.series_name+":::"+x.actors+":::"+x.director+":::"+x.f_tab+":::"+x.description+":::"+x.f_edit_time)
        MyLogger.debug(s"SeriesSaveToHdfs file:$saveFile ")
        try{
             seriesRDD.saveAsTextFile(saveFile)
        }catch{
             case e:Exception => {e.printStackTrace();MyLogger.debug("err"+e)}
             MyLogger.error(s"saveToHdfs $saveFile err")
        }
    }
    //直播点播数据
    def InitEvent(sc:SparkContext)={
        val sqlContext = new org.apache.spark.sql.SQLContext(sc)
        val jdbcModel = new showJdbc("event_series",DtvsJDBCUrl,DtvsUser,DtvsPasswd)
        eventSeries = jdbcModel.getEventSeries(sc,sqlContext).collect()
        jdbcModel.updateInfo("homed_eit_schedule",DtvsJDBCUrl,DtvsUser,DtvsPasswd)
        if(ReConfig.calcEitTomorrow == 1){
          val calendar = Calendar.getInstance()
          val ymdFormat = new SimpleDateFormat("yyyy-MM-dd")
         
          val today = ymdFormat.format(calendar.getTime)
          calendar.add(Calendar.DAY_OF_YEAR, +2)
          val dayAfterTomorrow = ymdFormat.format(calendar.getTime)
          MyLogger.debug(s"only calc eitschedule ,tomorrow")
          jdbcModel.setCondition(Array(s"duration < '24:00:00' and  start_time >='${today}' and start_time < '${dayAfterTomorrow}'"))
        }else{
          jdbcModel.setCondition(Array("duration < '24:00:00'"))
        }
        val eitScheduleRDD = jdbcModel.getEitSchedule(sc,sqlContext)
        
        jdbcModel.setCondition(Array(" orig_duration >= 0 and duration < '24:00:00'"))
        jdbcModel.updateInfo("homed_eit_schedule_history",DtvsJDBCUrl,DtvsUser,DtvsPasswd)
        val eitScheduleHistoryRDD = jdbcModel.getEitScheduleHistory(sc,sqlContext)
        //val eventSRDD = SeriesLoadToHdfs(sc)
        jdbcModel.setCondition(Array())
        jdbcModel.updateInfo("event_series",DtvsJDBCUrl,DtvsUser,DtvsPasswd)
        val eventSRDD = jdbcModel.getEventSeries(sc, sqlContext)
        eitScheduleHs = eitScheduleHistoryRDD.collect()
        eitSchedules = eitScheduleRDD.collect()
        
        findSeriesFromEitschedule(sc,eitScheduleRDD,eventSRDD)
        findSeriesFromEitscheduleHistory(eitScheduleHs,eventSeries)
        EventSeriesSaveToHdfs(sc)
    }
    def loadEventSeriesFromHdfs(sc:SparkContext)={
        val file1 = RTool.getSaveFileName("eventSeries", true)
        val file2 = RTool.getSaveFileName("eventSeries", false)
        var savedFile = ""
        MyLogger.debug(s"SeriesLoadToHdfs file:$savedFile ")
        val savedRdd = ListBuffer[RDD[(Int,Long,Long)]]()
        try{
          val fs = FileSystem.get(new Configuration())
          if(fs.exists(new Path(file2))) savedFile = file2 else savedFile = file1
          if(fs.exists(new Path(savedFile))){
            MyLogger.debug(s"has file $savedFile")
            savedRdd += sc.textFile(savedFile).map(line=>{
                line.split(":::", 3)
            }).filter(x=>x.length == 3).map(x=>(x(0).toInt,x(1).toLong,x(2).toLong))
          }
        }
        catch{
           case e:Exception => {
             e.printStackTrace();
             MyLogger.error("err"+e)
           }
        }finally {
          
        }
        sc.union(savedRdd)
    }
     //save to hdfs
    private def EventSeriesSaveToHdfs(sc:SparkContext)={
        val saveFile = RTool.getSaveFileName("eventSeries", false)
        val eventRDD = sc.makeRDD(eitScheduleToSeries.toList).map(x=>"1"+":::"+x._1.event_id+":::"+x._2.series_id)
        val eventhRDD = sc.makeRDD(eitSchedulehToSeries.toList).map(x=>"2"+":::"+x._1.event_id+":::"+x._2.series_id)
        val urdd = sc.union(eventRDD, eventhRDD)
        MyLogger.debug(s"SeriesSaveToHdfs file:$saveFile ")
        try{
             urdd.saveAsTextFile(saveFile)
        }catch{
             case e:Exception => {e.printStackTrace();MyLogger.debug("err"+e)}
             MyLogger.error(s"saveToHdfs $saveFile err")
        }
    }
    
    def Init(sc:SparkContext)={
        val sqlContext = new org.apache.spark.sql.SQLContext(sc)
        //剧集
        val jdbcModel = new showJdbc("event_series",DtvsJDBCUrl,DtvsUser,DtvsPasswd)
        eventSeries = jdbcModel.getEventSeries(sc,sqlContext).collect()
        //直播
        jdbcModel.updateInfo("homed_eit_schedule",DtvsJDBCUrl,DtvsUser,DtvsPasswd)
        if(ReConfig.calcEitTomorrow == 1){
          val calendar = Calendar.getInstance()
          val ymdFormat = new SimpleDateFormat("yyyy-MM-dd")
         
          val today = ymdFormat.format(calendar.getTime)
          calendar.add(Calendar.DAY_OF_YEAR, +2)
          val dayAfterTomorrow = ymdFormat.format(calendar.getTime)
          MyLogger.debug(s"only calc eitschedule ,tomorrow")
          jdbcModel.setCondition(Array(s"duration < '24:00:00' and  start_time >='${today}' and start_time < '${dayAfterTomorrow}'"))
        }else{
          jdbcModel.setCondition(Array("duration < '24:00:00'"))
        }
        eitSchedules = jdbcModel.getEitSchedule(sc,sqlContext).collect()
        //回看
        jdbcModel.setCondition(Array(" orig_duration >= 0 and duration < '24:00:00'"))
        jdbcModel.updateInfo("homed_eit_schedule_history",DtvsJDBCUrl,DtvsUser,DtvsPasswd)
        eitScheduleHs = jdbcModel.getEitScheduleHistory(sc,sqlContext).collect()
        findSeriesFromEitscheduleHistory(eitScheduleHs,eventSeries)
    }
    private def getVideoDescription(sc:SparkContext)={
        val sqlContext = new org.apache.spark.sql.SQLContext(sc)
        val jdbcModel = new videoDesJdbc("d_asset",DataJDBCUrl,DataUser,DataPasswd)
        val trainRdd = jdbcModel.getTrainInfoFromd_asset(sc, sqlContext)
        //jdbcModel.updateInfo("d_bundle", DateJDBCurl,DtvsUser,DtvsPasswd)
       // val trainRddBundle = jdbcModel.getTrainInfoFromd_bundle(sc, sqlContext)
        jdbcModel.updateInfo("d_program", DataJDBCUrl,DataUser,DataPasswd)
        val trainRddProgram = jdbcModel.getTrainInfoFromd_program(sc, sqlContext)
       // jdbcModel.updateInfo("l_liveShow_info", DateJDBCurl,DtvsUser,DtvsPasswd)
       // val liveinfo = jdbcModel.getLiveShowInfo(sc, sqlContext)
        val unionRdd = sc.union(trainRdd,trainRddProgram)//.union(liveinfo)
        unionRdd
    }
    //打印结果
    private def printInitResult()={
        MyLogger.debug("mergeEventSeries :")
        mergeEventSeries.foreach(x=>{MyLogger.debug(s"${x.series_id},${x.series_name},${x.actors},${x.director},${x.description}")})
        MyLogger.debug("eitSchedules :")
        eitSchedules.foreach(x=>{MyLogger.debug(s"${x.event_id},${x.event_name}")})
        MyLogger.debug("eitScheduleHs :")
        eitScheduleHs.foreach(x=>{MyLogger.debug(s"${x.event_id},${x.event_name}")})
        MyLogger.debug(s"all size:${mergeEventSeries.size},${eitSchedules.size},${eitScheduleHs.size}")
        MyLogger.debug("eitScheduleToSeries:")
        eitScheduleToSeries.foreach(x=>{
          MyLogger.debug(s"${x._1.event_name},${x._1.event_id} >>>>")
          if(x._2 != null)MyLogger.debug(s"${x._2.series_id},${x._2.series_name}") else MyLogger.debug("null")
        })
        MyLogger.debug("eitSchedulehToSeries:")
        eitSchedulehToSeries.foreach(x=>{
          MyLogger.debug(s"${x._1.event_name},${x._1.event_id} >>>>")
          if(x._2 != null)MyLogger.debug(s"${x._2.series_id},${x._2.series_name}") else MyLogger.debug("null")
        })
      
        MyLogger.debug(s"all size: ${mergeEventSeries.size},${eventSeries.size},${eitSchedules.size},${eitScheduleHs.size},${eitScheduleToSeries.size},${eitSchedulehToSeries.size}")
    }
    
   
    private def findSeriesFromEitHistory(x: homed_eit_schedule_history)={
        
    }
    private def findSeriesFromEitscheduleHistory(eith:Array[homed_eit_schedule_history],series:Array[event_serie])={
        eitSchedulehToSeries = Map()
        val seriesMap = series.map(x=>x.series_id->x).toMap
        eith.foreach(x=>{
             if(x.f_series_id == 0){
                // val serie = findSeriesFromEitHistory(x)
                 eitSchedulehToSeries += ((x,null))
             }
             else{
                 val serie = seriesMap.getOrElse(x.f_series_id, null)
                 eitSchedulehToSeries += ((x,serie))
             }
        })
        val seriesNotNull = eitSchedulehToSeries.filter(x=>x._2 != null)
        MyLogger.debug(s"seriesNotNull notnull:${seriesNotNull.size},total:${eitSchedulehToSeries.size}")
        eitSchedulehToSeries.foreach(x=>{
          if(x._2 != null){
              MyLogger.debug(s"${x._1.event_name},${x._1.event_id}---> ${x._2.series_name},${x._2.series_id}")
          }else{
              MyLogger.debug(s"${x._1.event_name},${x._1.event_id}---> null")
          }
        })
        eitSchedulehToSeries = seriesNotNull
    }
    implicit def cmp1: Ordering[(Long, Double)] = Ordering.by[(Long, Double), Double](_._2)
    
   // val desResult = ardd.cartesian(brdd).filter(x=>(calcSimilar(x._1, x._2)>0.75f))
    private def findSeriesFromEitschedule(sc:SparkContext,eitRDD:RDD[homed_eit_schedule],seriesRDD:RDD[event_serie])={
        eitScheduleToSeries = Map()
        val stime = System.currentTimeMillis()
        eitRDD.cache
        seriesRDD.cache
       // val a = eitRDD.map(x=>(x.event_id,x.event_name))
        val updateBroad = sc.broadcast(eitRDD.collect().map(x=>(x.event_id,x.event_name)))
        val seriesMap = seriesRDD.map(x=>x.series_id->x).collectAsMap
        
        val tmp = seriesRDD.map(x=>{
            updateBroad.value.map(y=>{
                (y._1->(x.series_id,RTool.calcSimilar(y._2, x.series_name)))
            })
        }).flatMap(x=>x).aggregateByKey(ListBuffer[(Long, Double)]())((u, v) => u+=v, (u1, u2) => u1 ++ u2).mapValues(a=>a.toList.max)
        val eitMap = eitSchedules.map(x=>x.event_id->x).toMap
        
        val desResult = tmp.map(x=>(eitMap.getOrElse(x._1,null),seriesMap.getOrElse(x._2._1, null),x._2._2)).collect
        val notokResult = desResult.filter(x=>x._3 <= 0.75f)
        MyLogger.debug("notokResult")
        notokResult.foreach(x=>MyLogger.debug(s"${x._1.event_name}-->${x._2.series_name},${x._3}"))
        /*val desResult = a.cartesian(seriesRDD).map(x=>
          (x._1->(x._2,RTool.calcSimilar(x._1._2, x._2.series_name)))
        ).aggregateByKey(ListBuffer[(event_serie, Double)]())((u, v) => u+=v, (u1, u2) => u1 ++ u2).mapValues(a=>a.toList.max).map(x=>(x._1,x._2._1)).collect
        */
        MyLogger.debug("desResult.size="+desResult.size)
        eitScheduleToSeries ++= desResult.map(x=>(x._1->x._2)).toMap
        
        MyLogger.debug(s"find ${eitScheduleToSeries.size},total: ${eitRDD.count()}\n")
        eitScheduleToSeries.foreach(x=>MyLogger.debug(s"${x._1.event_name},${x._1.event_id}--->${x._2.series_name},${x._2.series_id}"))
        MyLogger.debug("task findSeriesFromEitschedule run:"+(System.currentTimeMillis() - stime)/1000.0/60+"min")
    }
    implicit def cmp: Ordering[(String, Double)] = Ordering.by[(String, Double), Double](_._2)
    
    private def findDes(que:Array[String],library:RDD[String],log:String)={
        val t = library.map(x=>{
           que.map(y=>{
             (y->(x,RTool.calcSimilar(x, y)))
             //关键词相似效果不好
             //(y->(x,RTool.calcSimilarByWords(x, y)))
           })
        })
        t.flatMap(x=>x).aggregateByKey(ListBuffer[(String, Double)]())((u, v) => u+=v, (u1, u2) => u1 ++ u2).mapValues(a=>a.toList.max).map(x=>(x._1,x._2._1,x._2._2))
    }
    //通过videoDes查找eventSeries对应的tab和描述
    private def findShowInfo(sc:SparkContext,vdesRDD:RDD[videoDes],eventRDD:RDD[event_serie])={
        val stime = System.currentTimeMillis()
        vdesRDD.cache()
        eventRDD.cache()
        MyLogger.debug(s"Datasize:${vdesRDD.count()},${eventRDD.count()}")
        val a = eventRDD.map(x=>x.series_name).distinct()
        val updateBroad = sc.broadcast(a.collect())
        val videoDesMap = vdesRDD.map(x=>x.showName->x).collectAsMap
        val desResult = findDes(updateBroad.value, vdesRDD.map(_.showName), "findDes").collect
        val notOkResult = desResult.filter(x=>x._3 < 0.75f)
        val eventSeriesMap = eventRDD.map(x=>(x.series_name,x)).collectAsMap 
        
        mergeEventSeries.clear()
        MyLogger.debug("notOkResult:"+notOkResult.size)
        notOkResult.foreach(x=>MyLogger.debug(x.toString()))
        MyLogger.debug("videoDes")
        videoDesMap.foreach(x=>MyLogger.debug(s"${x._2.showName},${x._2.labelOrTags},${x._2.actors},${x._2.directors},${x._2.description}"))
        MyLogger.debug("seriesMap")
        eventSeriesMap.foreach(x=>MyLogger.debug(s"${x._2.series_name}"))
        MyLogger.debug(s"desResult.size=${desResult.size},seriesSize=${eventSeriesMap.size}")
       // val saveToHdfsMerge = ListBuffer[event_serie]()
        val p = Pattern.compile("\\s*|\t|\r|\n");
       
        desResult.foreach(x=>{
            val einfo = eventSeriesMap.getOrElse(x._1, null)
            val videodes = videoDesMap.getOrElse(x._2,null)
            if(einfo != null && videodes != null){
                //格式化一下字符串，去掉换行等字符
                var formatDes = ""
                if(videodes.description != null){
                  val m = p.matcher(videodes.description)
                 // MyLogger.debug("format before:"+videodes.description)
                  formatDes = m.replaceAll("")
                //  MyLogger.debug("format after:"+formatDes)
                }
                MyLogger.debug(s"${RTool.livNameformat(einfo.series_name)},${einfo.series_name}--->>${videodes.showName}")
                mergeEventSeries += event_serie(einfo.series_id,einfo.series_name,einfo.service_id,videodes.actors,videodes.directors,formatDes,videodes.labelOrTags,einfo.f_edit_time)
                /*if(x._3 >= 1.0){
                    saveToHdfsMerge += event_serie(einfo.series_id,einfo.series_name,einfo.service_id,videodes.actors,videodes.directors,videodes.description,videodes.labelOrTags,einfo.f_edit_time)
                }*/
            }
        })
        MyLogger.debug(s"find ${mergeEventSeries.size},total: ${a.count()}\n")
        MyLogger.debug("task findShowInfo run:"+(System.currentTimeMillis() - stime)/1000.0/60+"min")
        mergeEventSeries
    }
    private def validfeature(fea:(String,Int)):Boolean={
        if(fea._1 == null || fea._1 == "null" || fea._1.contains("null") || fea._1.length() >5) return false
        return true
    }
    def findSeriesFeature(sc:SparkContext)={
        val seriesRDD = SeriesLoadToHdfs(sc)
        val serieFeatureRDD = seriesRDD.map(serie=>{
             val tmp = RTool.gettabList(serie.f_tab).map(x=>(x,5))++ RTool.gettabList(serie.actors + serie.director).map(x=>(x,3)) ++ Classification.getKextractKeyWord(serie.series_name,serie.description).map(x=>(x,1))
            (serie.series_name,tmp.filter(validfeature(_)))
        })
        serieFeatureRDD.cache()
        //去掉频率少的特征
        val allFeaturesRDD = serieFeatureRDD.flatMap(x=>x._2).map(x=>(x._1,1)).reduceByKey(_+_).filter(validfeature(_)).filter(x=>x._2 > 1).map(_._1)
        val allFeatures = allFeaturesRDD.collect
        MyLogger.debug(s"allFeatures[${allFeatures.size}]::${allFeatures.mkString("|")}")
        val serieFeatureList = serieFeatureRDD.collect()
        MyLogger.debug(s"serieFeatureList::")
        serieFeatureList.foreach(x=>{
            MyLogger.debug(s"${x._1}")
            x._2.foreach(x=>MyLogger.debug(x.toString()))
        })
        def featureToDouble(tabList:Array[(String,Int)])={
             val tmap = tabList.toMap
             allFeatures.map(x=>{tmap.getOrElse(x,0).toDouble})
            // tabList.map(x=>if(serieFeatureList.contains(x._1)) x._2.toDouble else 0.0).toArray
        }
        val FeatureNumRDD = serieFeatureRDD.map(x=>(x._1,featureToDouble(x._2)))
        MyLogger.debug("debug FeatureNumRDD")
        val debug = FeatureNumRDD.map(x=>x._1+"::"+x._2.mkString("|")).collect.foreach(x=>MyLogger.debug(x.toString()))
        SeriesFeatureSaveToHdfs(sc,FeatureNumRDD)
    }
    //load 
    def SeriesFeatureLoadToHdfs(sc:SparkContext)={
        var savedFile =""
        val file1  = RTool.getSaveFileName("feature", true)
        val file2  = RTool.getSaveFileName("feature", false)
        
        MyLogger.debug(s"SeriesFeatureLoadToHdfs file:$savedFile ")
        val savedRdd = ListBuffer[RDD[(String,Array[Int])]]()
        try{
          val fs = FileSystem.get(new Configuration())
          if(fs.exists(new Path(file2))) savedFile = file2 else savedFile = file1
          
          if(fs.exists(new Path(savedFile))){
            MyLogger.debug(s"has file $savedFile")
            savedRdd += sc.textFile(savedFile).map(line=>{
                line.split(":::", 2)
            }).filter(x=>x.length == 2).map(x=>(x(0),x(1).split(",").map(x=>x.toInt)))
          }
        }
        catch{
           case e:Exception => {
             e.printStackTrace();
             MyLogger.debug("err"+e)
           }
        }finally {
        }
        sc.union(savedRdd)
    }
    //save to hdfs
    private def SeriesFeatureSaveToHdfs(sc:SparkContext,feature:RDD[(String,Array[Double])])={
        val saveFile = RTool.getSaveFileName("feature", false)
        MyLogger.debug(s"SeriesFeatureSaveToHdfs file:$saveFile ")
        val featureRDD = feature.map(x=>(x._1,x._2.mkString(","))).map(x=>x._1+":::"+x._2+"\n")

        try{
             featureRDD.saveAsTextFile(saveFile)
        }catch{
             case e:Exception => {e.printStackTrace();MyLogger.debug("err"+e)}
             MyLogger.debug(s"saveToHdfs $saveFile err")
        }
    }
    
    def getAllEvent()={
        val a:Array[homed_eit_schedule_history] = eitSchedules
        eitScheduleHs ++ a
    }
    
    def geteitScheduleToSeries()={
         eitScheduleToSeries.toMap
    }
    def geteitSchedulehToSeries()={
         eitSchedulehToSeries.toMap
    }
    def geteitScheduleHistory()={
         eitScheduleHs
    }
    def geteitSchedule()={
         eitSchedules
    }
    def geteventSeries()={
         eventSeries
    }
    def getSeriesToScheduleList()={
         SeriesToScheduleList
    }
    
    def getEventSeriesFullInfo()={
         mergeEventSeries
    }
    def getChannelInfo()={
        ChannelInfo
    }
    
   
}