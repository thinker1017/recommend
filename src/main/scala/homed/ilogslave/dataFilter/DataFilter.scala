package homed.ilogslave.dataFilter

import org.apache.spark.SparkContext
import homed.Jdbc._
import homed.config.ReConfig._
import scala.collection.mutable.ListBuffer
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.rdd.JdbcRDD
import homed.ilogslave.dataWarehouse._
import homed.ilogslave.dataWarehouse.userBehavior._
import org.apache.commons.collections.iterators.FilterListIterator
import homed.tools._

//数据过滤
object DataFilter {
    
    //event_id 是否挂在栏目树了
    private var validProgramMap = scala.collection.mutable.Map[Long,t_column_program]()
    //series 是否挂在栏目树了
    private var validSeriesMap = Map[Long,Long]()
    private var eventSeriesMap = Map[Long,event_serie]()
    private var eitScheduleHs = Array[homed_eit_schedule_history]()
    private var validSeriesList = List[Long]()
    private var allevent =  Array[homed_eit_schedule_history]()
    private var alleventSeries = Array[event_serie]()
    private var eventIdNameMap = Map[Int, (String,Long,Long,Long)]()
    private var seriesIdNameMap = Map[Long, (String,Long,Long,Long)]()
    
    def Init(sc:SparkContext)={
        val columProgram = columnInfo.getColumnProgram().map(x=>(x.f_program_id,x)).toMap
        val duplicateProgram = columnInfo.getDuplicateProgram
        val duplicateSeries = columnInfo.getDuplicateSeries()
        eitScheduleHs = Live.geteitScheduleHistory()
        val duplicateMap = (duplicateProgram.map(x=>(x.f_program_id->x.f_duplicate_id)) ++ duplicateSeries.map(x=>(x.f_main_program_id->x.f_duplicate_id))).toMap
        eitScheduleHs.foreach(x=>{
          val dupid = duplicateMap.getOrElse(x.f_series_id, 0L)
          val cp = columProgram.getOrElse(dupid, null)
          if(cp != null && dupid != 0)
            validProgramMap += ((x.event_id.toLong->cp))
        })
        validSeriesMap = (duplicateProgram.map(x=>(x.f_program_id,x.f_duplicate_id)) ++ duplicateSeries.map(x=>(x.f_main_program_id,x.f_duplicate_id))).map(x=>(x._1->x._2)).filter(x=>columProgram.contains(x._2)).toMap
        validSeriesList = validProgramMap.map(x=>x._1).toList
        
        allevent = Live.getAllEvent
        alleventSeries = Live.geteventSeries()
        eventIdNameMap = allevent.map(x=>x.event_id->(x.event_name,x.event_id.toLong,x.start_time.getTime,x.duration.getTime)).toMap
        seriesIdNameMap = alleventSeries.map(x=>x.series_id->(x.series_name,x.series_id,0L,0L)).toMap
        MyLogger.debug("allevent:"+allevent.size+",alleventSeries:"+alleventSeries.size)
        printResult
    }
    
    private def printResult()={
        MyLogger.debug("validProgramMap:")
        validProgramMap.foreach(x=>MyLogger.debug(x.toString()))
        MyLogger.debug(s"all size : ${validProgramMap.size}")
        MyLogger.debug("validSeriesMap:")
        validSeriesMap.foreach(x=>MyLogger.debug(x.toString()))
        MyLogger.debug(s"all size : ${validSeriesMap.size}")
    }
    
    def filterSimrecommend(sc:SparkContext,arr:Array[t_itemcf_recommend])={
      
        val filterarr  = arr.filter(p=>validProgramMap.contains(p.f_recommend_id))
        MyLogger.debug(s"filter sim ${arr.size - filterarr.size}")
        filterarr
    }
    def filterShowRelate(sc:SparkContext,arr:Array[t_show_relate])={
        val filterarr = arr.filter(x=>(x.f_program_id != 0 && x.f_series_id != 0))
        MyLogger.debug(s"relate filtered:${arr.size - filterarr.size}")
        filterarr
    }
    
    def filtertabrecommend(sc:SparkContext,arr:Array[t_content_recommend])={
        val filterarr = arr.filter(p=>( p.f_index < 255 && (validSeriesMap.contains(p.f_recommend_id) || (p.f_recommend_id > PROGRAM_EVENT_ID_BEGIN && p.f_recommend_id < PROGRAM_EVENT_ID_END))))
        
        MyLogger.debug(s"tabrecommend filtered ${arr.size - filterarr.size}")
        filterarr
    }
    
    def filterInvalieSeries(arr:List[t_content_recommend])={
        val filterarr = arr.filter(p=>validSeriesMap.contains(p.f_recommend_id)).filter(p=>p.f_index < 255)
        
        MyLogger.debug(s"tabrecommend filtered ${arr.size - filterarr.size}")
        filterarr
    }
    //过滤掉推荐的相同节目，比如晚7点，很多频道很播新闻，这个时候就会推荐出几个相同内容的推荐
    def filterSameProgram(arr:Array[t_content_recommend])={
        
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
           // MyLogger.debug("li._2,"+li._2.mkString("|"))
            //MyLogger.debug("t="+(live++event).mkString("|"))
            event ++ live
        }).map(_._2).toList
        MyLogger.debug(arr.size+",filtered = "+filtered.size+",recomEvent="+recomEvent.size)
        val filterList = arr.filter(x=>filtered.contains(x.f_recommend_id))
        val filteredList = arr.toSet &~ filterList.toSet
        MyLogger.debug(s"filterdsame :")
        filteredList.foreach(x=>MyLogger.debug("filter:"+x))
        filterList
    }
    
    def validSeries(serieID:Long)={
        validSeriesMap.contains(serieID)
    }

}