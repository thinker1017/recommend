package homed.ilogslave
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
import homed.ilogslave.dataWarehouse._
import homed.ilogslave.dataWarehouse.userBehavior._
import homed.ilogslave.recommendAlgorithm._
import homed.Jdbc._
import homed.ilogslave.dataFactory.DataFactory
import homed.ilogslave.dataFilter.DataFilter
import homed.ilogslave.recommendAlgorithm._


 
//直播推荐     
object Liverecommend {
    private var userSeries = Map[Long, List[event_serie]]()
    private var simSeries = Map[Long, List[(Long, Double)]]()
    private var itemrecommendList = ListBuffer[t_itemcf_recommend]()
    implicit def EventToVideo(eit: event_serie) = video_serie(eit.series_id,eit.series_name,eit.service_id,5,"",
        1,eit.actors,eit.director,"内地","普通话",eit.description,1,"",eit.f_edit_time,eit.f_tab,80,"")
    
    private var m_eventSeries = Map[Long,event_serie]()
    
    private var m_event = Map[Long,homed_eit_schedule]()
    private var m_eventh = Map[Long,homed_eit_schedule_history]()
    //eitSchedule与eventseries对应完善节目资料
    private var eitScheduleToSeries = scala.collection.Map[homed_eit_schedule,event_serie]()
    
     //eitSchedulehistory与eventseries对应完善节目资料
    private var eitSchedulehToSeries = scala.collection.Map[homed_eit_schedule_history,event_serie]()
    //剧集对应的直播节目list
    private var SeriesToScheduleList = scala.collection.Map[event_serie,List[homed_eit_schedule]]()
    
    private def InitData(sc:SparkContext)={
        Live.Init(sc)
        Live.InitChannel(sc)
        columnInfo.Init(sc)
        DataFactory.Init(sc)
        DataFilter.Init(sc)
       // Classification.Init(sc)
     
    }
    
    private def ContentBasedInitData(sc:SparkContext)={
        val eventToSeries = Live.loadEventSeriesFromHdfs(sc).collect()
       
        m_eventSeries = Live.geteventSeries().map(x=>x.series_id->x).toMap
        m_event = Live.geteitSchedule().map(x=>x.event_id.toLong->x).toMap
        m_eventh = Live.geteitScheduleHistory().map(x=>x.event_id.toLong->x).toMap
        eitSchedulehToSeries = Map()
        eitScheduleToSeries = Map()
        eventToSeries.foreach(s=>{
            if(s._1 == 1){//直播
                val event = m_event.getOrElse(s._2, null)
                val series = m_eventSeries.getOrElse(s._3, null)
                if(event != null && series != null){
                    eitScheduleToSeries += ((event,series))
                    MyLogger.debug(s"etoSeries ${event.event_name}-->${series.series_name}}")
                }else{
                    MyLogger.debug(s"err edata :$s,$event,$series")
                }
                
            }else{
                val event = m_eventh.getOrElse(s._2, null)
                val series = m_eventSeries.getOrElse(s._3, null)
                if(event != null && series != null){
                    eitSchedulehToSeries += ((event,series))
                    MyLogger.debug(s"ehtoSeries ${event.event_name}-->${series.series_name}}")
                }else{
                    MyLogger.debug(s"err ehdata :$s,$event,$series")
                }
            }
        })
        MyLogger.debug(s"ContentBasedInitRelation:: data Size ${m_eventSeries.size},${m_event.size},${m_eventh.size}")
    }
 
    private def ContentBasedSaveToHdfs(sc:SparkContext,t_content:RDD[t_content_recommend])={
        val calendar = Calendar.getInstance()
        val ymdFormat = new SimpleDateFormat("yyyy-MM-dd")
        val dateStr = if(SAVINGFILEDATA == "00-00-00") ymdFormat.format(calendar.getTime) else SAVINGFILEDATA
        val saveFile = ilogslaveRddSavePrefix+dateStr+"/"+"contentBasedRecommend"+".data"+SAVINGDATAINDEX
        val disRdd = t_content.map(x=>x.f_program_id+":::"+x.f_index+":::"+x.f_recommend_id+":::"+x.f_recommend_reason)
        MyLogger.debug(s"ContentBasedSaveToHdfs file:$saveFile ")
        try{
             disRdd.saveAsTextFile(saveFile)
        }catch{
             case e:Exception => {e.printStackTrace();MyLogger.debug("err"+e)}
             MyLogger.debug(s"saveToHdfs $saveFile err")
        }
    }
    private def ContentBasedRecommendDate()={
        val calendar = Calendar.getInstance()
        val ymdFormat = new SimpleDateFormat("yyyy-MM-dd")
         
        val today = ymdFormat.format(calendar.getTime)
        calendar.add(Calendar.DAY_OF_YEAR, +2)
        val dayAfterTomorrow = ymdFormat.format(calendar.getTime)
        MyLogger.debug(s"ContentBasedRecommendDate :$dayAfterTomorrow")
        dayAfterTomorrow
    }
    //hdfs 存的是名称，这里将相同剧集名称加上相似向量
    private def supplementSameNameSim(sc:SparkContext,sim:Map[Long, List[(Long, Double)]])={
        val m_SeriesNameListMap = sc.makeRDD(m_eventSeries.toList).map(x=>x._2.series_name->x._2).aggregateByKey(ListBuffer[event_serie]())((u, v) => u+=v, (u1, u2) => u1 ++ u2).mapValues(a=>a.toList).collectAsMap()
        val supple = ListBuffer[(Long, List[(Long, Double)])]()
        sim.foreach(s=>{
            val t = m_eventSeries.getOrElse(s._1, null)
            val sameNameSeries = m_SeriesNameListMap.getOrElse(t.series_name, null)
            if(sameNameSeries.size > 1){
                val t = sameNameSeries.filter(x=>x.series_id != s._1).map(x=>(x.series_id,(s._2)))
                supple ++= t
            }
        })
        MyLogger.debug("supple size ="+supple.size)
        supple.toMap ++ sim
    }
    def ContentBasedRecommendEx(sc:SparkContext)={
        InitData(sc)
        ContentBasedInitData(sc)
        var m_SeriesName = m_eventSeries.map(x=>RTool.formatShowName(x._2.series_name)->x._2)
        val itemsim = CB.ContentBased_loadSim(sc).map(x=>(x._1->(x._2,x._3))).aggregateByKey(ListBuffer[(String,Double)]())((u, v) => u+=v, (u1, u2) => u1 ++ u2).mapValues(a=>a.toArray).collectAsMap().toMap
        val t_content = DataFactory.ProduceContentBaseLiveRecommendEx(sc,m_eventSeries.map(_._2).toArray,m_event.map(_._2).toArray,m_eventh.map(_._2).toArray,itemsim)
        
        resultJdbc.savetabrecommend(sc,t_content.toArray,ContentBasedRecommendDate)
        ContentBasedSaveToHdfs(sc,sc.makeRDD(t_content))
        //MyLogger.debug("kmeans test:")
       // Kmeans.kMeansBytabScore(sc)
    }
    //
    def ContentBasedRecommend(sc:SparkContext)={
        InitData(sc)
        ContentBasedInitData(sc)
        var m_SeriesName = m_eventSeries.map(x=>RTool.formatShowName(x._2.series_name)->x._2)
        val itemsim = CB.ContentBased_loadSim(sc).map(x=>m_SeriesName.getOrElse(x._1,null)->(m_SeriesName.getOrElse(x._2, null),x._3)).filter(x=>(x._1 != null && x._2._1 != null)).map(x=>x._1.series_id->(x._2._1.series_id,x._2._2)).aggregateByKey(ListBuffer[(Long,Double)]())((u, v) => u+=v, (u1, u2) => u1 ++ u2).mapValues(a=>a.toList).collectAsMap()
        val suppleItemSim = supplementSameNameSim(sc,itemsim.toMap)
        //val itemsim = CB.ContentBased_loadSim(sc).map(x=>m_SeriesName.getOrElse(x._1,null)->(m_SeriesName.getOrElse(x._2, null),x._3)).filter(x=>(x._1 != null && x._2._1 != null)).map(x=>x._1.series_id->(x._2._1.series_id,x._2._2)).aggregateByKey(ListBuffer[(Long,Double)]())((u, v) => u+=v, (u1, u2) => u1 ++ u2).mapValues(a=>a.toList).collectAsMap()
        MyLogger.debug("suppleItemSim")
        suppleItemSim.foreach(x=>MyLogger.debug(x.toString()))
        val calendar = Calendar.getInstance()
        val ymdFormat = new SimpleDateFormat("yyyy-MM-dd")
        val today = ymdFormat.format(calendar.getTime)
        calendar.add(Calendar.DAY_OF_YEAR, +1)
        val dayAfterTomorrow = ymdFormat.format(calendar.getTime)
        
        val t_content = DataFactory.ProduceContentBaseLiveRecommendData(sc,m_eventSeries.map(_._2).toArray,m_event.map(_._2).toArray,m_eventh.map(_._2).toArray,suppleItemSim.toMap,eitScheduleToSeries.toMap)
        resultJdbc.savetabrecommend(sc,t_content.toArray,ContentBasedRecommendDate)
        ContentBasedSaveToHdfs(sc,sc.makeRDD(t_content))
        
    }  
    
    def ItemCfRecommendInitData(sc:SparkContext)={
        Live.Init(sc)
        userInfo.Init(sc)
        videoSeries.Init(sc)
        userSeries = UserHit.getLiveHits(sc).toMap
    }
    def ItemCfRecommend(sc:SparkContext)={
        ItemCfRecommendInitData(sc)
    }
    
    private def calcUserportrait(sc:SparkContext)={
      MyLogger.debug("calcUserportrait")
      val videohit = UserHit.getUserVideoHitList
      videohit.foreach(x=>MyLogger.debug(x.toString()))
      Userportrait.Init()
      Userportrait.putBehavior(videohit)
      Userportrait.calcProtrait(sc)
      MyLogger.debug("calcUserportrait end")
      
    }
    
    
    def Init(sc:SparkContext)={
        
        //settabrecommend(sc)
       // calcUserportrait(sc)
    }
   
}