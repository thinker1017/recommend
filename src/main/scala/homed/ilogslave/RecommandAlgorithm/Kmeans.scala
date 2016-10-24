package homed.ilogslave.recommendAlgorithm

import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.SparkContext
import org.apache.spark.rdd._
import org.apache.spark.sql.SQLContext
import homed.config.ReConfig
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating
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
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors

object Kmeans {
  
  
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
             featureList.map(x=>{tmap.getOrElse(x,0).toDouble})
            // tabList.map(x=>if(serieFeatureList.contains(x._1)) x._2.toDouble else 0.0).toArray
        }
        val FeatureNumRDD = itemFeatureRDD.map(x=>(x._1,featureToDouble(x._2)))
        FeatureNumRDD
    }
    //获取要计算的节目名称（直播和剧集节目名，去重）
    def getShowNameList(sc:SparkContext)={
        val sqlContext = new org.apache.spark.sql.SQLContext(sc)
        val jdbcModel = new showJdbc("event_series",DtvsJDBCUrl,DtvsUser,DtvsPasswd)
        val SeriesSRDD = jdbcModel.getEventSeries(sc,sqlContext)
        
        val calendar = Calendar.getInstance()
        val ymdFormat = new SimpleDateFormat("yyyy-MM-dd")
        val today = ymdFormat.format(calendar.getTime)
        calendar.add(Calendar.DAY_OF_YEAR, +2)
        val dayAfterTomorrow = ymdFormat.format(calendar.getTime)
        MyLogger.debug(s"only calc eitschedule ,tomorrow")
        
        jdbcModel.updateInfo("homed_eit_schedule",DtvsJDBCUrl,DtvsUser,DtvsPasswd)
        jdbcModel.setCondition(Array(s"duration < '24:00:00' and  start_time >='${today}' and start_time < '${dayAfterTomorrow}'"))
        val eventRDD = jdbcModel.getEitSchedule(sc, sqlContext)
        
        val seriesUnionEvent = sc.union(SeriesSRDD.map(x=>RTool.formatShowName(x.series_name)), eventRDD.map(x=>RTool.formatShowName(x.event_name))).distinct()
        seriesUnionEvent
        // jdbcModel.updateInfo("homed_eit_schedule_history",DtvsJDBCUrl,DtvsUser,DtvsPasswd)
       // val eventHRDD = jdbcModel.getEitScheduleHistory(sc, sqlContext)
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
             MyLogger.debug("err"+e)
           }
        }
        sc.union(savedRdd)
    } 
    
    //以广播方式计算sim
    def kMeansBytabScore(sc:SparkContext,needCalcFeature: RDD[(String, Array[Double])])={
        
        val vectorRdd = needCalcFeature.map(x => Vectors.dense(x._2))
        val numClusters = 6
        val numIterations = 20
        val clusters = KMeans.train(vectorRdd, numClusters, numIterations)
        val ClusterList = ListBuffer[(String,Int)]()
        needCalcFeature.collect.foreach(fa=>{
            val clusterID = clusters.predict(Vectors.dense(fa._2))
            ClusterList += ((fa._1,clusterID))
        })
        val result = sc.makeRDD(ClusterList).map(x=>x._2->x._1).aggregateByKey(ListBuffer[(String)]())((u, v) => u+=v, (u1, u2) => u1 ++ u2).mapValues(a=>a.toList).collectAsMap()
        result.foreach(p=>{
            MyLogger.debug("cluster:"+p._1)
            p._2.foreach(x=>MyLogger.debug(x.toString()))
        })
        MyLogger.debug("kMeansBytabScore done")

    }

}