package homed.ilogslave.dataFactory
import homed.Jdbc._
import scala.collection.mutable.ListBuffer
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.rdd.JdbcRDD
import homed.ilogslave.dataWarehouse._
import homed.ilogslave.dataWarehouse.userBehavior._
import homed.ilogslave.recommendAlgorithm._
import homed.ilogslave.dataFilter.DataFilter
import java.lang.Math
import homed.ilogslave.recommendAlgorithm.CB
import homed.config.ReConfig._
import homed.tools._

//数据工厂
object DataFactory {
    
    //eitSchedule与seriesid对应
    private var eitScheduleToSeries = Map[Long,event_serie]()
    
    //eitSchedulehistory与seriesid对应
    private var eitSeriesToScheduleh = Map[Long,event_serie]()
    
    private var seriesToschedules = scala.collection.Map[Long,List[homed_eit_schedule]]()
    private var seriesToschedulehs = scala.collection.Map[Long,List[homed_eit_schedule_history]]()
    private var recommendMap = Map[Long,t_itemcf_recommend]()
    private var tabMap = Map[Long,t_content_recommend]()
    private val coldStartData = List("综艺","悬疑","真人秀","家庭","体育","动漫","少儿")
    private var IDRECOMMENDSIZE = 100
    private var tabShowMap = Map[String, List[event_serie]]()
    private var hotSeries = List[event_serie]()
    private val date = new java.sql.Date(new java.util.Date().getTime)
    private var showIndex = Map[Long,Int]()
    
    implicit def eitToEith(x: homed_eit_schedule_history ) = homed_eit_schedule(x.homed_service_id,x.event_id,x.event_name,x.start_time,x.duration)
    
    //private var channelInfo
    private val channelSim = Map(
      List("卡通","少儿","卡酷","幼儿")->1,
      List("新闻","经济","中央二台","财经")->2,
      List("体育","CCTV-5","中央五台")->3,
      List("电视剧","中央八台","综艺")->4,
      List("CCTV-11","戏曲")->5
    )

    
    def Init(sc:SparkContext)={
       // hotSeries = UserHit.getHotSeriesByWC()
    }
   
    private def getHistoryShow(series:Long)={
        val eit = seriesToschedulehs.getOrElse(series, null)
        MyLogger.debug(s"eit=$eit")
        if(eit != null && eit.size > 0) eit(0) else null
    }
    private def getFutureShow(event_id:Long)={
         val eith = seriesToschedules.getOrElse(event_id, null)
         if(eith != null && eith.size > 0) eith(0) else null
    }
    private def getshowIndex(id:Long)={
        val a = showIndex.getOrElse(id, 0)
        showIndex += ((id,a+1))
        a
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
   
    
    private def filterAddFill(event_id:Long,stab:String,filterList: List[t_content_recommend])={
        val recommendList =  DataFilter.filterInvalieSeries(filterList)
        var index  = filterList.size
        val fillList = ListBuffer[t_content_recommend]()
        var needFill = IDRECOMMENDSIZE - recommendList.size
        if(hotSeries.size <= 0) hotSeries  = UserHit.getHotSeriesByWC()
        while(needFill > 0 && hotSeries.size > 0 && index < 255){
          val cblist = ListBuffer[event_serie]()
          try{
            val randomNum = scala.util.Random.nextInt(hotSeries.size)
            val tab = Classification.getKextractKeyWord((hotSeries(randomNum).series_name,hotSeries(randomNum).actors),20)
            tab.foreach(x=>{
                if(tabShowMap.contains(x)){
                   val t = tabShowMap.getOrElse(x, null)
                   cblist ++= t.take(2)
                }
            })
            cblist.foreach(series=>{
                fillList += t_content_recommend(event_id,stab,getshowIndex(event_id),series.series_id,series.f_edit_time,series.f_tab,new java.sql.Timestamp(date.getTime))
            })
          }catch{
            case e:Exception => {e.printStackTrace();MyLogger.debug("err"+e)}
          }
          needFill -= (if(cblist.size > 1)cblist.size  else 1)
        }
        MyLogger.debug("fill list:")
        fillList.distinct.foreach(x=>MyLogger.debug(x.toString()))
        recommendList ++ fillList.distinct
    }
    //直播节目推荐补充
    private def liveFill(eit: homed_eit_schedule,has:Array[Long],total:Array[Int],tsize:Int)={
        val remain = total.filter(x=> !has.contains(x.toLong)).map(x=>(x.toLong,0.1)).take(tsize)
        MyLogger.debug(s"liveFill,${eit.event_name}"+remain)
        remain.toList
    }
    
    private def getItemSimList(eit: homed_eit_schedule,e_serie :event_serie,itemsim: Map[Long, List[(Long, Double)]],seriesEventMap: Map[Long, List[Long]],validlivingEitList: Map[Int, Array[Int]],takeSize:Int)={
        val simSeries = itemsim.getOrElse(e_serie.series_id, null).filter(_!= null).sortWith((s,t)=>s._2 > t._2)
        //推荐正在直播的节目, //直播节目按相似
        val simLiving = simSeries.map(x=>{
            seriesEventMap.getOrElse(x._1, List(0L)).map(e=>(e,x._2))
        }).flatten.sortWith((s,t)=>s._2 > t._2)//.map(_._1).toArray
                 
        val liveEitByTime = validlivingEitList.getOrElse(eit.event_id, null)
        val livingByScore = simLiving.filter(x=>liveEitByTime.contains(x._1.toInt)).take(takeSize).map(x=>(x._1,x._2 * LiveWeight))
        //val livingByScore = liveEitByTime.filter(x=>simLiving.contains(x)).take(100).map(x=>x.toLong)
        MyLogger.debug("livingByScore="+livingByScore.mkString("|"))
        //推荐回看
        val eventByScore = simSeries.filter(x=>DataFilter.validSeries(x._1)).take(takeSize).map(x=>(x._1,x._2 * EventWeight))
        MyLogger.debug("eventByScore="+eventByScore.mkString("|"))
        
        val sortByWeight = (livingByScore ++ eventByScore)
        val AddFileList = if(sortByWeight.length >= takeSize) List() else liveFill(eit,sortByWeight.map(_._1).toArray,liveEitByTime,takeSize - sortByWeight.size)
        MyLogger.debug("sortByWeight="+sortByWeight.mkString("|"))
        MyLogger.debug(s"living recommend size:${eit.event_name},living:${livingByScore.size},event:${eventByScore.size},add:${AddFileList.size}")
        sortByWeight ++ AddFileList
    }
    //剧集没有对应相似剧集
    private def liveFillWithoutSimSeries(series:Array[event_serie])={
        val seriesList = ListBuffer[Int]()
        for(i <- 0 to 10)
          seriesList += scala.util.Random.nextInt(series.size)
        seriesList.distinct
    }
    //直播节目没有找到对应的剧集
    private def liveFillWithoutcorrespondingSeries(series:Array[event_serie])={
        val seriesList = ListBuffer[Int]()
        for(i <- 0 to 10)
          seriesList += scala.util.Random.nextInt(series.size)
        seriesList.distinct
    }
    
    private def liveWithNoSim(eit:homed_eit_schedule,series:Array[event_serie],live:Array[homed_eit_schedule],tsize:Int)={
        val recommendList = ListBuffer[t_content_recommend]()
        val halfsize = tsize/2
        for(i <- 0 to halfsize){
          val num1 = scala.util.Random.nextInt(series.size)
          recommendList += t_content_recommend(eit.event_id,"",0,series(num1).series_id,series(num1).f_edit_time,series(num1).series_name,new java.sql.Timestamp(date.getTime))
          val num2 = scala.util.Random.nextInt(live.size)
          recommendList += t_content_recommend(eit.event_id,"",0,live(num2).event_id,live(num2).start_time,live(num2).event_name,new java.sql.Timestamp(date.getTime))
        }
        MyLogger.debug("live With NO sim:"+eit.event_name+","+RTool.formatShowName(eit.event_name))
        recommendList.foreach(x=>MyLogger.debug(x.toString()))
        recommendList.toArray
    }
    implicit def cmp1: Ordering[(String, Double)] = Ordering.by[(String, Double), Double](_._2)
    
    def test(sc:SparkContext)={
        val rdd = sc.textFile("1.txt", 1)
        val aid = rdd.flatMap { line =>
            val (id,name) = line.span(_!='\t')
            if(name.isEmpty())
              None
            else {
                try{
                  Some((id.toInt),name.trim())
                }catch{
                  case e:NumberFormatException => None
                }
                
            }
        }
    }
    def ProduceContentBaseLiveRecommendEx(sc:SparkContext,series:Array[event_serie],live:Array[homed_eit_schedule],liveh:Array[homed_eit_schedule_history],itemsim: Map[String, Array[(String, Double)]])={
        //剧集不考虑重名情况，正好可以排除重名推荐
        val seriesformatNameMap  = series.map(x=>RTool.formatShowName(x.series_name)->x).toMap
        val eitRDD = sc.makeRDD(live)
        eitRDD.cache()
        val eitFormatNameRDD = eitRDD.map(x=>RTool.formatShowName(x.event_name))
        //直播需要考虑重名情况
        val eitFormatNameEventMap = eitRDD.map(x=>RTool.formatShowName(x.event_name)->x).aggregateByKey(ListBuffer[(homed_eit_schedule)]())((u, v) => u+=v, (u1, u2) => u1 ++ u2).mapValues(a=>a.toList).collectAsMap()
        //直播节目对应的正在直播的其他节目
        val validlivingEitList = live.map(s=>s.event_id->live.filter(validLivingShow(s,_)).map(x=>x.event_id)).toMap
        //初始化推荐结果的index
        
        val eitIdMap = live.map(x=>x.event_id->x).toMap
        val seriesIdMap = series.map(x=>x.series_id->x).toMap 
        showIndex = Map()
        val channelInfo = Live.getChannelInfo.map(x=>x.channel_id->x.chinese_name).toMap
        val eitchannelMap = live.map(x=>x.event_id->channelInfo.getOrElse(x.homed_service_id,"")).toMap
      
        //根据播放频道打分
        def simChannelScore(a:Long,b:Long):Double={
            val sa = eitchannelMap.getOrElse(a.toInt,"x")
            val sb = eitchannelMap.getOrElse(b.toInt,"xx")
            var s1 = 0
            var s2 = -1
            for(p <- channelSim){
                for(channel <- p._1){
                    if(sa.contains(channel)){
                        s1 = p._2
                    }
                    if(sb.contains(channel)){
                        s2 = p._2
                    }
                } 
            }
            //MyLogger.debug(s"channelScore:$sa,$sb,$s1,$s2")
            
            //根据频道得到的类型完全不同，将减分
            if(s1 == s2) return 1
            else{
               // MyLogger.debug(s"not sim category:$a,$b,$s1,$s2")
                return 0.1
            }
        }
        
        val t_contentRecommend = live.map(eit=>{
            val formatShow = RTool.formatShowName(eit.event_name)
            var f_result = Array[t_content_recommend]()
            if(!itemsim.contains(formatShow)){
                MyLogger.debug(s"err data live has no sim:${eit.event_name}")
                f_result = liveWithNoSim(eit,series,live,200)
            }else{
              
                
                MyLogger.debug("---------------------")
                MyLogger.debug("eit="+eit.event_name+","+eit.event_id)
                val liveEitByTime = validlivingEitList.getOrElse(eit.event_id, null)
                
                val simShow = itemsim.getOrElse(formatShow, null).sortWith((s,t)=>s._2 > t._2).take(500).distinct
                MyLogger.debug("sim"+simShow.mkString("|"))
                
                val liveSimList = simShow.filter(x=>eitFormatNameEventMap.contains(x._1)).map(x=>eitFormatNameEventMap.getOrElse(x._1,null).map((_,x._2 * LiveWeight))).flatten.filter(x=>liveEitByTime.contains(x._1.event_id))
                //liveSimList.foreach(x=>MyLogger.debug("liv:"+x._1.event_name+","+x._2))
                val eventSimList = simShow.filter(x=>seriesformatNameMap.contains(x._1)).map(x=>(seriesformatNameMap.getOrElse(x._1,null),x._2 * EventWeight)).filter(_._1 != null)
                //eventSimList.foreach(x=>MyLogger.debug("ser:"+x._1.series_name+","+x._2))
                
                val t2 = liveSimList.take(200).map(x=>(t_content_recommend(eit.event_id,"",0,x._1.event_id,x._1.start_time,x._1.event_name,new java.sql.Timestamp(date.getTime)),RTool.formatShowName(x._1.event_name),x._2))
                val t1 = eventSimList.filter(x=>DataFilter.validSeries(x._1.series_id)).take(200).map(x=>(t_content_recommend(eit.event_id,"",0,x._1.series_id,x._1.f_edit_time,x._1.f_tab,new java.sql.Timestamp(date.getTime)),x._1.series_name,x._2))
                val liveShow = t2.map(_._2)
                //去掉相同节目的推荐，优先选择live
                val filtert1 = t1.filter(t=> !liveShow.contains(RTool.formatShowName(t._2))).map(x=>(x._1,x._3))
                
                //根据节目播放频道进行相似估计
                val withChannelSim = (t2.map(x=>(x._1,x._3)) ++ filtert1).map(x=>(x._1,simChannelScore(x._1.f_program_id,x._1.f_recommend_id)*x._2))
                val result = withChannelSim.sortWith((s,t)=>s._2 > t._2)
                //result.foreach(x=>MyLogger.debug(x.toString()))
                result.foreach(x=>
                if(x._1.f_recommend_id > PROGRAM_SERIES_EVENT_ID_BEGIN && x._1.f_recommend_id < PROGRAM_SERIES_EVENT_ID_END){
                    val t = seriesIdMap.getOrElse(x._1.f_recommend_id,null)
                    MyLogger.debug(s"debug series:${x._1.f_recommend_id},${t.series_name},${t.series_id}")
                }else{
                    val t = eitIdMap.getOrElse(x._1.f_recommend_id.toInt,null)
                    MyLogger.debug(s"debug living:${x._1.f_recommend_id},${t.event_name},${t.event_id}")
                })
                f_result = result.map(_._1).distinct
                if(f_result.size < 200){
                      MyLogger.debug("warning recommend list is small"+f_result.size)
                      f_result ++= liveWithNoSim(eit,series,live,200 - f_result.size)
                      MyLogger.debug("supplement size"+f_result.size)
                }
            }
            val re = f_result.take(200).map(x=>t_content_recommend(x.f_program_id,x.f_tag_name,getshowIndex(x.f_program_id),x.f_recommend_id,x.f_recommend_date,x.f_recommend_reason,x.f_recommend_date))
            val filtered = DataFilter.filterSameProgram(re)
            MyLogger.debug("filtered:"+re.size+","+filtered.size)
            filtered
        }).flatMap(x=>x)
        
        
        /*val t_contentRecommendh = liveh.map(eit=>{
            val formatShow = RTool.formatShowName(eit.event_name)
            val result = if(!itemsim.contains(formatShow)){
                MyLogger.debug(s"err data live has no sim:${eit.event_name}")
                liveWithNoSim(eit,series,live,200)
            }else{
                var re = Array[t_content_recommend]()
                try{
                  MyLogger.debug("---------------------")
                  MyLogger.debug("eit="+eit.event_name+","+eit.event_id)
                  val liveEitByTime = validlivingEitList.getOrElse(eit.event_id, null)
                  
                  val simShow = itemsim.getOrElse(formatShow, null).sortWith((s,t)=>s._2 > t._2).take(500).distinct
                  MyLogger.debug("sim"+simShow.mkString("|"))
                  
                  val liveSimList = simShow.filter(x=>eitFormatNameEventMap.contains(x._1)).map(x=>eitFormatNameEventMap.getOrElse(x._1,null).map((_,x._2 * LiveWeight))).flatten.filter(x=>liveEitByTime.contains(x._1.event_id))
                  //liveSimList.foreach(x=>MyLogger.debug("liv:"+x._1.event_name+","+x._2))
                  val eventSimList = simShow.filter(x=>seriesformatNameMap.contains(x._1)).map(x=>(seriesformatNameMap.getOrElse(x._1,null),x._2 * EventWeight)).filter(_._1 != null)
                  eventSimList.foreach(x=>MyLogger.debug("ser:"+x._1.series_name+","+x._2))
                  
                  val t2 = liveSimList.take(200).map(x=>(t_content_recommend(eit.event_id,"",0,x._1.event_id,x._1.start_time,x._1.event_name,new java.sql.Timestamp(date.getTime)),RTool.formatShowName(x._1.event_name),x._2))
                  val t1 = eventSimList.filter(x=>DataFilter.validSeries(x._1.series_id)).take(200).map(x=>(t_content_recommend(eit.event_id,"",0,x._1.series_id,x._1.f_edit_time,x._1.f_tab,new java.sql.Timestamp(date.getTime)),x._1.series_name,x._2))
                  val liveShow = t2.map(_._2)
                  //去掉相同节目的推荐，优先选择live
                  val filtert1 = t1.filter(t=> !liveShow.contains(RTool.formatShowName(t._2))).map(x=>(x._1,x._3))
                  
                  //根据节目播放频道进行相似估计
                  val withChannelSim = (t2.map(x=>(x._1,x._3)) ++ filtert1).map(x=>(x._1,simChannelScore(x._1.f_program_id,x._1.f_recommend_id)*x._2))
                  val result = withChannelSim.sortWith((s,t)=>s._2 > t._2)
                  //result.foreach(x=>MyLogger.debug(x.toString()))
                  result.foreach(x=>
                  if(x._1.f_recommend_id > PROGRAM_SERIES_EVENT_ID_BEGIN && x._1.f_recommend_id < PROGRAM_SERIES_EVENT_ID_END){
                      val t = seriesIdMap.getOrElse(x._1.f_recommend_id,null)
                      MyLogger.debug(s"debug series:${x._1.f_recommend_id},${t.series_name},${t.series_id}")
                  }else{
                      val t = eitIdMap.getOrElse(x._1.f_recommend_id.toInt,null)
                      MyLogger.debug(s"debug living:${x._1.f_recommend_id},${t.event_name},${t.event_id}")
                  })
                  MyLogger.debug("#####################")
                  var r_re = result.map(_._1).distinct
                  if(r_re.size < 40){
                      MyLogger.debug("warning recommend list is small"+r_re.size)
                      r_re ++= liveWithNoSim(eit,series,live,50)
                      MyLogger.debug("supplement size"+r_re.size)
                  }
                  re = r_re
                }catch{
                    case e:Exception => {
                       e.printStackTrace();
                       MyLogger.debug("err")
                       re = liveWithNoSim(eit,series,live,50)
                    }
                }
                re
            }
            val re = result.take(200).map(x=>t_content_recommend(x.f_program_id,x.f_tag_name,getshowIndex(x.f_program_id),x.f_recommend_id,x.f_recommend_date,x.f_recommend_reason,x.f_recommend_date))
            val filtered = DataFilter.filterSameProgram(re)
            MyLogger.debug("filtered:"+re.size+","+filtered.size)
            filtered
        }).flatMap(x=>x)
       */
       // val filtered_t_contentRecommend =  DataFilter.filterSameProgram(t_contentRecommend.toList,allevent)
      //  MyLogger.debug(t_contentRecommend.size+","+"filtered_t_contentRecommend size="+filtered_t_contentRecommend.size)
        //val recommendList = t_contentRecommend
        MyLogger.debug("debug::::")
       
        val debug = sc.makeRDD(t_contentRecommend ).map(x=>x.f_program_id->x).aggregateByKey(ListBuffer[(t_content_recommend)]())((u, v) => u+=v, (u1, u2) => u1 ++ u2).mapValues(a=>a.toList).map(x=>(eitIdMap.getOrElse(x._1.toInt,null),x._2)).collectAsMap()
        debug.foreach(tt=>{
            MyLogger.debug("eit:"+tt._1.event_name+","+tt._1.event_id)
            tt._2.foreach(x=>
              if(x.f_recommend_id > PROGRAM_SERIES_EVENT_ID_BEGIN && x.f_recommend_id < PROGRAM_SERIES_EVENT_ID_END){
                  val t = seriesIdMap.getOrElse(x.f_recommend_id,null)
                  MyLogger.debug(s"re series:${x.f_recommend_id},${t.series_name},${channelInfo.getOrElse(t.service_id,"")}")
              }else{
                  val t = eitIdMap.getOrElse(x.f_recommend_id.toInt,null)
                  MyLogger.debug(s"re living:${x.f_recommend_id},${t.event_name},${eitchannelMap.getOrElse(t.event_id,"")}")
              }
              
            )
        })
        
        t_contentRecommend
    }
    
    //第一版接口
    def ProduceContentBaseLiveRecommendData(sc:SparkContext,series:Array[event_serie],live:Array[homed_eit_schedule],events:Array[homed_eit_schedule_history],itemsim: Map[Long, List[(Long, Double)]],itemSeries:Map[homed_eit_schedule,event_serie])={
        MyLogger.debug("ProduceContentBaseLiveRecommendData")
        val seriesEventMap = sc.makeRDD(itemSeries.toList).map(x=>x._2.series_id->x._1.event_id).aggregateByKey(ListBuffer[(Long)]())((u, v) => u+=v, (u1, u2) => u1 ++ u2).mapValues(a=>a.toList).collectAsMap().toMap
        val liveRDD = sc.makeRDD(live)
        val liveMap = live.map(x=>x.event_id->x).toMap
        val seriesMap = series.map(x=>x.series_id->x).toMap
        val itemIdSeriesName = itemSeries.map(x=>x._1.event_id->x._2.series_name)
        //直播节目对应的正在直播的其他节目
        val validlivingEitList = live.map(s=>s.event_id->live.filter(validLivingShow(s,_)).map(x=>x.event_id)).toMap
        val recommendByScore = ListBuffer[(Long,Array[Long])]()
        live.map(x=>{
            val e_serie = itemSeries.getOrElse(x, null)
            var sortByWeight = Array[Long]()
            if(e_serie != null){//直播对应找到了剧集
                MyLogger.debug(s"living find series ${x.event_name},${e_serie.series_id},${e_serie.series_name}")
                if(itemsim.contains(e_serie.series_id)){//剧集有对应的相似列表
                    sortByWeight =getItemSimList(x,e_serie,itemsim,seriesEventMap,validlivingEitList,100).sortWith((s,t)=>s._2 > t._2).map(_._1).toArray
                }else{
                    sortByWeight = liveFillWithoutSimSeries(series).map(series(_).series_id).toArray
                    MyLogger.debug(s"err data series has no simList:$e_serie,recommend size=${sortByWeight.size}")
                }
            }else{
                sortByWeight = liveFillWithoutcorrespondingSeries(series).map(series(_).series_id).toArray
                MyLogger.debug(s"err data live has no series:$x,recommend Size:${sortByWeight.size}")
            }
            //直播节目对应的剧集只推荐一个，不要重复的
            val distinctSeries = sortByWeight.map(x=>(itemIdSeriesName.getOrElse(x.toInt, x.toString()),x)).toMap.map(x=>x._2).toArray
            recommendByScore += ((x.event_id.toLong,sortByWeight.filter(distinctSeries.contains(_))))
        })
        MyLogger.debug("recommendByScore:")
        showIndex = Map()
        val l_content_recommend = ListBuffer[t_content_recommend]()
        recommendByScore.foreach(x=>{
            val rlist = x._2
            val live = liveMap.getOrElse(x._1.toInt, null)
            MyLogger.debug(s"eit:${live.event_name}")
            rlist.foreach(e=>{
                if(e > PROGRAM_SERIES_EVENT_ID_BEGIN && e < PROGRAM_SERIES_EVENT_ID_END){
                    val serie = seriesMap.getOrElse(e, null)
                    if(serie == null){
                      MyLogger.debug(s"err serie ${e}")
                    }else{
                      l_content_recommend += t_content_recommend(x._1,"",getshowIndex(x._1),serie.series_id,serie.f_edit_time,serie.f_tab,new java.sql.Timestamp(date.getTime))
                      MyLogger.debug(s"re series:${serie.series_id},${serie.series_name}")
                    }
                }else{
                    val eit = liveMap.getOrElse(e.toInt, null)
                    if(eit == null){
                      MyLogger.debug(s"err living event ${e}")
                    }else{
                      l_content_recommend += t_content_recommend(x._1,"",getshowIndex(x._1),eit.event_id,eit.start_time,"liveBySim",new java.sql.Timestamp(date.getTime))
                      MyLogger.debug(s"re living:${eit.event_id},${eit.event_name}")
                    }
                }
            })
        })
        l_content_recommend
        //val recomend_contentRDD = recommendByScore.map()
    }
}