package homed.ilogslave.recommendAlgorithm

import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.SparkContext
import org.apache.spark.rdd._
import org.apache.spark.sql.SQLContext
import homed.config.ReConfig
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.SingularValueDecomposition
import java.text.SimpleDateFormat
import java.util.Calendar
import java.sql.DriverManager
import org.apache.spark.rdd.RDD.doubleRDDToDoubleRDDFunctions
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import homed.ilogslave.dataWarehouse._
import homed.tools._
import scala.math._
import homed.config.ReConfig._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import homed.Jdbc._

object CB {
  
    private var tabShowMap = scala.collection.Map[String,List[event_serie]]()
    private val coldStartData = List("综艺","悬疑","真人秀","家庭","体育","动漫","少儿")
    private var videoTabMap = Map[(String, String), Array[String]]()
    private var seriesMap = Map[(String,String),event_serie]()
    private val importantCategory = List("体育","动画","新闻","综艺","财经","戏曲","影视综艺","纪录片","生活","其他")
    
    private var allItemFeatureStrMap =  Map[String, Array[String]]()  
    private var allFeatureScoreMap = Map[String, Double]()
    private var allShowCateMap = Map[String,String]()
    private var CateShowMap = Map[String,List[String]]()
    private var allFeaturesList = Array[String]()
    
    def gettabShowMap()={
        tabShowMap
    }
    //基于内容推荐之节目tab
    def ContentBased_tag_Init(sc:SparkContext,arr:Array[event_serie])={
        val tabShowList = ListBuffer[(String,event_serie)]()
        arr.foreach(x=>{
            val tab = Classification.getKextractKeyWord((x.series_name,x.actors),20)
            tab.foreach(t=>(
                tabShowList += ((t,x))
            ))
        })
        //tabShowMap = 
        tabShowMap = sc.makeRDD(tabShowList).aggregateByKey(ListBuffer[(event_serie)]())((u, v) => u+=v, (u1, u2) => u1 ++ u2).mapValues(a=>a.toList).collectAsMap
        videoTabMap = Classification.getvideoTabMap().toMap
        seriesMap = Live.getEventSeriesFullInfo().map(x=>(x.series_name,x.actors)->x).toMap
    } 
    def calcTabScore(a:Array[String],ned:Array[String])={
        val insec = a.toSet & ned.toSet
        insec.size/(ned.length*1.0)
    }
    
    def ContentBased_tag_getResult(sc:SparkContext,series:event_serie)={
        val cblist  = ListBuffer[event_serie]()
        val tabList = Classification.getKextractKeyWord((series.series_name,series.actors),20)
        MyLogger.debug(s"tagresult: ${tabList.mkString("|")}")
        
        if(tabList.size <= 0){//对应节目tab没有，找热门
            coldStartData.foreach(x=>{
               if(tabShowMap.contains(x))
                 cblist ++= tabShowMap.getOrElse(x, null)
               MyLogger.debug(s"no tab,use cold data")
            })
        }
        else{
            val avideos = videoTabMap.map(x=>{
                 (x._1,calcTabScore(x._2,tabList))
            }).toList.sortWith((s,t)=>s._2 > t._2).map(x=>seriesMap(x._1)).filter(x=>x != null).take(20)
            cblist ++= avideos
            
        }
        MyLogger.debug(s"tabresult:${cblist}")
        cblist.distinct
    }
    //欧式距离
    private def EuclideanDistance(attr1:Array[Double],attr2:Array[Double]) :Double={
        var sum = 0.0
        for(i <- 0 to attr1.length-1){
          sum += (attr1(i) - attr2(i))*(attr1(i) - attr2(i))
        }
        1.0/(1.0+sqrt(sum))
    }
    //余弦相似度
    private def cosineSim(attr1:Array[Int],attr2:Array[Int]) :Double={
        var sum = 0.0
        var ds1 = 0.0
        var ds2 = 0.0
        for(i <- 0 to attr1.length-1){
          sum += attr1(i) * attr2(i)
          ds1 += attr1(i)*attr1(i)
          ds2 += attr2(i)*attr2(i)
        }
        return sum/(sqrt(ds1)*sqrt(ds2))
    }
   // implicit def cmp: Ordering[(String, (String,Double))] = Ordering.by[(String, String,Double)), Double](_._2)
    private def validfeature(fea:(String,Int)):Boolean={
        if(fea._1 == null || fea._1 == "null" || fea._1.contains("null") || fea._1.length() >5) return false
        return true
    }
    
    //读取tagSystem 得到的节目标签数据
    private def loadShowTraitDes(sc:SparkContext)={
        var savedFile = ""
        val file1 = RTool.getSaveFileName("traitDes", true) 
        val file2 = RTool.getSaveFileName("traitDes", false)
        val savedRdd = ListBuffer[RDD[(String,Array[String])]]()
        try{
          val fs = FileSystem.get(new Configuration())
          if(fs.exists(new Path(file2))) savedFile = file2 else savedFile = file1
          if(fs.exists(new Path(savedFile))){
            MyLogger.debug(s"has file $savedFile")
            savedRdd += sc.textFile(savedFile).map(line=>{
                line.split(":::", 2)
            }).filter(x=>x.length == 2).map(x=>(x(0),x(1).split(",")))
          }
        }
        catch{
           case e:Exception => {
             e.printStackTrace()
             MyLogger.debug("err"+e)
           }
        }finally {
        }
        sc.union(savedRdd)
    }
    def FeatureNumericalMatrix(itemFeatureRDD:RDD[(String,Array[(String,Int)])],featureList:Array[String])={
        
        def featureToDouble(tabList:Array[(String,Int)])={
             val tmap = tabList.toMap
             featureList.map(x=>{tmap.getOrElse(x,0)})
            // tabList.map(x=>if(serieFeatureList.contains(x._1)) x._2.toDouble else 0.0).toArray
        }
        val FeatureNumRDD = itemFeatureRDD.map(x=>(x._1,featureToDouble(x._2)))
        FeatureNumRDD
    }
    
    private def featureToDouble(tabList:Array[(String,Int)],featureList:Array[String])={
        val tmap = tabList.toMap
        featureList.map(x=>{tmap.getOrElse(x,0)})
    }
    def FeatureNumericalMatrix(itemFeatureList:Array[(String,Array[(String,Int)])],featureList:Array[String])={
        itemFeatureList.map(x=>(x._1,featureToDouble(x._2,featureList)))
    }
    
    //获取要计算的节目名称（直播和剧集节目名，去重）
    def getShowNameList(sc:SparkContext)={
        val sqlContext = new org.apache.spark.sql.SQLContext(sc)
        val jdbcModel = new showJdbc("event_series",DtvsJDBCUrl,DtvsUser,DtvsPasswd)
        val SeriesSRDD = jdbcModel.getEventSeries(sc,sqlContext)
        
        /*val calendar = Calendar.getInstance()
        val ymdFormat = new SimpleDateFormat("yyyy-MM-dd")
        val today = ymdFormat.format(calendar.getTime)
        calendar.add(Calendar.DAY_OF_YEAR, +2)
        val dayAfterTomorrow = ymdFormat.format(calendar.getTime)
        MyLogger.debug(s"only calc eitschedule ,tomorrow")
        */
        jdbcModel.updateInfo("homed_eit_schedule",DtvsJDBCUrl,DtvsUser,DtvsPasswd)
       // jdbcModel.setCondition(Array(s"duration < '24:00:00' and  start_time >='${today}' and start_time < '${dayAfterTomorrow}'"))
        val eventRDD = jdbcModel.getEitSchedule(sc, sqlContext)
        
        val seriesUnionEvent = sc.union(SeriesSRDD.map(x=>RTool.formatShowName(x.series_name)), eventRDD.map(x=>RTool.formatShowName(x.event_name))).distinct()
        seriesUnionEvent
        // jdbcModel.updateInfo("homed_eit_schedule_history",DtvsJDBCUrl,DtvsUser,DtvsPasswd)
       // val eventHRDD = jdbcModel.getEitScheduleHistory(sc, sqlContext)
    }
    //针对主要类别分类再计算相似度
    def classifier(sc:SparkContext,itemFea: RDD[(String, Array[String])])={
        val showCate = itemFea.flatMap(item=>{
            val t = importantCategory.flatMap(cate=>{
                if(cate == "影视综艺" && (item._2.contains(cate)|item._2.contains("影片")|item._2.contains("电视剧")||
                    item._2.contains("综艺")))
                  Some(item._1,cate)
                else if(item._2.contains(cate)) 
                  Some(item._1,cate)
                else
                  None
            })
            t
        })
        
        MyLogger.debug("itemFea:"+itemFea.count)
        val noneCate = itemFea.map(_._1).subtract(showCate.map(_._1)).distinct.collect()
        val othercate = noneCate.flatMap(x=>Some(x,"其他"))
        MyLogger.debug("noneCate:"+othercate.size)
        othercate.foreach(x=>MyLogger.debug(x.toString()))
        val cateShow = (showCate ++ sc.makeRDD(othercate)).map(_.swap).aggregateByKey(ListBuffer[(String)]())((u, v) => u+=v, (u1, u2) => u1 ++ u2).mapValues(a=>a.toList.distinct).collect
        cateShow.foreach(cate=>{
            MyLogger.debug("cate:"+cate._1)
            cate._2.foreach(x=>MyLogger.debug(cate._1+","+x))
        })
        allShowCateMap = showCate.collectAsMap().toMap
        CateShowMap = cateShow.toMap
        cateShow
    }
   /* def ContentBased_itemsSimInit(sc:SparkContext)={
        val itemFeatureStrRDD = loadShowTraitDes(sc).distinct()
        val featureScoreMap = loadFeatureScore(sc).collectAsMap()
        MyLogger.debug(s"featureScoreMap:")
        featureScoreMap.foreach(x=>MyLogger.debug(x.toString()))
        MyLogger.debug(s"itemFeatureStrRDD:")
        itemFeatureStrRDD.foreach(x=>MyLogger.debug(x._1+":::"+x._2.mkString("|")))
        val nameSimList = getShowNameList(sc).collect()
        val cateshow = classifier(sc,itemFeatureStrRDD)
        (itemFeatureStrRDD.collect,featureScoreMap,nameSimList,cateshow)
    }
    def ContentBased_itemsSimCalcSim()={
        
    }*/
   
    def ContentBased_itemsSimOne(show:String)={
        MyLogger.debug("[CB] calc sim "+show)
        val stime = System.currentTimeMillis()
        var simList = Array[(String, Double)]()
        if(allShowCateMap.contains(show)){
            var cate = allShowCateMap.getOrElse(show, null)
            MyLogger.info(s"[CB] $show : $cate")
            if (cate == null) {
                cate = "其他"
            }
            val sameCateList = CateShowMap.getOrElse(cate, null)
            if(sameCateList != null && sameCateList.size > 0){
                val calcshow = allItemFeatureStrMap.getOrElse(show, Array("")).map(y=>(y,allFeatureScoreMap.getOrElse(y, 1.0).toInt))
                val itemfeaList = allItemFeatureStrMap.filter(x => sameCateList.contains(x._1)).toArray
                val itemFeatureNumList = FeatureNumericalMatrix(itemfeaList.map(x=>(x._1,x._2.map(y=>(y,allFeatureScoreMap.getOrElse(y, 1.0).toInt)))),allFeaturesList)
                val _score = featureToDouble(calcshow,allFeaturesList)
                simList = itemFeatureNumList.map(item=>{
                    (item._1,cosineSim(item._2,_score))
                }).sortWith((s,t)=>s._2 > t._2).filter(_._2 != 0.0)
            }
            else{
                MyLogger.error("[CB] sameCateList is null")
            }
            
            
        }else{
            MyLogger.error("[CB] has no cate:"+show)
        }
        MyLogger.info(s"[CB] calc: $show")
        simList.foreach(x=>MyLogger.info(s"[CB] $x"))
        MyLogger.info(s"[CB] ContentBased_itemsSimOne end.. run:"+(System.currentTimeMillis() - stime)/1000.0+"seconds")
        
        simList
    }
    //以广播方式计算sim
    def ContentBased_itemsSimBroad(sc:SparkContext)={
        val itemFeatureStrRDD = loadShowTraitDes(sc).distinct()
        val featureScoreMap = loadFeatureScore(sc).collectAsMap()
        MyLogger.debug(s"featureScoreMap:")
        featureScoreMap.foreach(x=>MyLogger.debug(x.toString()))
        itemFeatureStrRDD.cache()
        val cateShow = classifier(sc,itemFeatureStrRDD)
        val nameSimList = getShowNameList(sc).collect()
        MyLogger.debug("nameSimList:")
        nameSimList.foreach(x=>MyLogger.debug("simname:"+x))
        
        //init map
        allItemFeatureStrMap = itemFeatureStrRDD.collectAsMap().toMap
        allFeatureScoreMap = featureScoreMap.toMap
        allFeaturesList = itemFeatureStrRDD.flatMap(x=>x._2).map(x=>(x,1)).reduceByKey(_+_).filter(validfeature(_)).filter(x=>x._2 > 3).map(_._1).collect
                 
        //因为少儿，电视剧，戏曲等区分性太差，如果全部拿来做相似性推荐，难免有不合理的问题，所以先做大分类，再分别进行相似计算，
        //其实当数据量过大就是先kmeans 再相似计算的
        def CalaSimByImportantCategory(cateItemFeatureStrRdd:RDD [(String, Array[String])])={
              
              val allFeaturesRDD = cateItemFeatureStrRdd.flatMap(x=>x._2).map(x=>(x,1)).reduceByKey(_+_).filter(validfeature(_)).filter(x=>x._2 > 3).map(_._1)
              val featureList = allFeaturesRDD.collect()
              val itemFeatureRDD = FeatureNumericalMatrix(cateItemFeatureStrRdd.map(x=>(x._1,x._2.map(y=>(y,featureScoreMap.getOrElse(y, 1.0).toInt)))),featureList)
              itemFeatureRDD.cache()
              MyLogger.debug(s"count=${itemFeatureRDD.count()},featureSize="+featureList.size)
              MyLogger.debug("itemFeatureRDD:")
              
              val updateBroad = sc.broadcast(itemFeatureRDD.collect())
              val filteredRdd = itemFeatureRDD.flatMap(item=>{
                    updateBroad.value.map(update=>{
                      (update._1->(item._1,cosineSim(item._2,update._2)))
                  })
              }).filter(x=>(x._1 != x._2._1))
              
              filteredRdd.cache()
              val saveRdd = filteredRdd.map(x=>(x._1,x._2._1,x._2._2))
              saveRdd
             
        }
        val saveSimRdd = cateShow.map(cates=>{
            MyLogger.debug("cala cate:"+cates._1)
            val caterdd = itemFeatureStrRDD.filter(x=>(cates._2.contains(x._1) && nameSimList.contains(x._1)))
            
            CalaSimByImportantCategory(caterdd).filter(_._3 > 0.0)
        })
        val totalSimRdd = sc.union(saveSimRdd)
        
        ContentBased_saveSim(sc,totalSimRdd)
        MyLogger.debug("save success")
        val simListRDD = totalSimRdd.map(x=>x._1->(x._2,x._3)).aggregateByKey(ListBuffer[(String, Double)]())((u, v) => u+=v, (u1, u2) => u1 ++ u2).mapValues(a=>a.toList.distinct.sortWith((s,t)=>s._2 > t._2))
        val simList = simListRDD.collect
        simList.foreach(x=>{
            MyLogger.debug(s"${x._1} simshow: size=${x._2.size}")
            x._2.foreach(x=>MyLogger.debug(x.toString()))
        })
        //较少相似节目
        val lesssim = simList.filter(_._2.size <= 100)
        lesssim.foreach(x=>{
            MyLogger.warn(s"warning ${x._1} simshow: size=${x._2.size}")
            x._2.foreach(x=>MyLogger.debug(x.toString()))
        })
        simList
    }
    def ContentBased_itemsSim(sc:SparkContext)={
       // val eventRDD = 
       // val itemFeatureStrRDD = loadShowTraitDes(sc)
        /*val itemFeatureRDD = Live.SeriesFeatureLoadToHdfs(sc)
        
       // itemFeatureStrRDD.cache()
        itemFeatureRDD.cache()
        MyLogger.debug(s"count=${itemFeatureRDD.count()}")
        val itemRDD = itemFeatureRDD.map(_._1)
        val itemNameList = itemFeatureRDD.map(x=>x._1).collect()
        var index  = 0
        val itemIndexMap = itemNameList.map(x=>{index = index + 1;(x,index+1)}).toMap
        val indexItemMap = itemIndexMap.map(x=>x._2->x._1)
       // val hdfssim = ContentBased_loadSim(sc).map(_._1)
        val needUpdateItem= itemRDD.collect()
        
        //物品索引，物品特征数组
        val itemattrsRDD = itemFeatureRDD.map(x=>(itemIndexMap.getOrElse(x._1, 1),x._2))
        val needUpdateItemAttrsRDD = itemFeatureRDD.filter(x=> needUpdateItem.contains(x._1)).map(x=>(itemIndexMap.getOrElse(x._1, 1),x._2))
        // （（物品A索引，物品B索引），（物品A属性数组，物品B属性数组））
        val carRdd = needUpdateItemAttrsRDD.cartesian(itemattrsRDD).map(x=>((x._1._1,x._2._1),(x._1._2,x._2._2)))
        val disRdd = carRdd.map(x=>(x._1,cosineSim(x._2._1,x._2._2)))
       // MyLogger.debug("disRdd")
       // disRdd.collect().foreach(x=>MyLogger.debug(x.toString()))
        val disNameRdd = disRdd.map(x=>((indexItemMap.getOrElse(x._1._1, ""),indexItemMap.getOrElse(x._1._2, ""),x._2))).
                    filter(x=>x._3 > 0.0).filter(x=>x._1 != x._2)
        disNameRdd.cache()
        MyLogger.debug("sim result:")
        val debug = disNameRdd.map(x=>x._1->(x._2,x._3)).aggregateByKey(ListBuffer[(String, Double)]())((u, v) => u+=v, (u1, u2) => u1 ++ u2).mapValues(a=>a.toList.sortWith((s,t)=>s._2 > t._2))
        
        debug.collect.foreach(x=>{
          MyLogger.debug(s"${x._1} simshow: size=${x._2.size}")
          x._2.foreach(x=>MyLogger.debug(x.toString()))
        })
        ContentBased_saveSim(sc,disNameRdd)*/
    }
    def ContentBased_loadSim(sc:SparkContext)={
        var savedFile = ""
        val file1 = RTool.getSaveFileName("disSim", true) 
        val file2 = RTool.getSaveFileName("disSim", false)
        MyLogger.debug(s"ContentBased_loadSim file:$savedFile ")
        val savedRdd = ListBuffer[RDD[(String, String, Double)]]()
        try{
          val fs = FileSystem.get(new Configuration())
          if(fs.exists(new Path(file2))) savedFile = file2 else savedFile = file1
          if(fs.exists(new Path(savedFile))){
            MyLogger.debug(s"has file $savedFile")
            savedRdd += sc.textFile(savedFile).map(line=>{
                line.split(":::", 3)
            }).filter(_.length == 3).map(x=>(x(0),x(1),x(2).toDouble))
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
    private def ContentBased_saveSim(sc:SparkContext,disNameRdd:RDD[(String, String, Double)])={
        val saveFile = RTool.getSavingFileName("disSim") 
        val disRdd = disNameRdd.map(x=>x._1+":::"+x._2+":::"+x._3)
        MyLogger.debug(s"ContentBased_saveSim file:$saveFile ")
        try{
             disRdd.saveAsTextFile(saveFile)
        }catch{
             case e:Exception => {e.printStackTrace();MyLogger.debug("err"+e)}
             MyLogger.error(s"saveToHdfs $saveFile err")
        }
    }
    //加载特征对应的分数，比如"剧情"分数是10，"科学"分数是8，该分数描述了此标签对一个节目的影响程度
    private def loadFeatureScore(sc:SparkContext)={
        var savedFile = ""
        val file1 = RTool.getSaveFileName("featureScore", true) 
        val file2 = RTool.getSaveFileName("featureScore", false)
        MyLogger.debug(s"loadFeatureScore file:$savedFile ")
        val savedRdd = ListBuffer[RDD[(String, Double)]]()
        try{
          val fs = FileSystem.get(new Configuration())
          if(fs.exists(new Path(file2))) savedFile = file2 else savedFile = file1
          if(fs.exists(new Path(savedFile))){
            MyLogger.debug(s"has file $savedFile")
            savedRdd += sc.textFile(savedFile).map(line=>{
                line.split(":::", 2)
            }).filter(_.length == 2).map(x=>(x(0),x(1).toDouble))
          }
        }
        catch{
           case e:Exception => {
             e.printStackTrace();
             MyLogger.error("err"+e)
           }
        }
        sc.union(savedRdd)
    }
    
    
    
    
    
    
}