package homed.ilogslave.recommendAlgorithm


import homed.ilogslave.dataWarehouse._
import homed.tools._
import org.apache.spark.SparkContext
import org.apache.spark.rdd._
import scala.collection.mutable.ListBuffer
import org.ansj.app.keyword.KeyWordComputer
import org.ansj.app.keyword.Keyword
import org.ansj.domain.Term;
import org.ansj.recognition.NatureRecognition
import org.ansj.util.MyStaticValue
import org.ansj.library.UserDefineLibrary
import homed.ilogslave.recommendAlgorithm._
import scala.collection.JavaConversions._
import breeze.linalg.{DenseVector, SparseVector, Vector}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix

trait VideoVector extends Vector[Double] with Serializable {
    def getVideoName(): String

    def toMLlibVector(): org.apache.spark.mllib.linalg.Vector

    override def toString(): String = {
      "VideoName: " + getVideoName() + " " + super.toString()
    }
}

class VideoDenseVector(videoName: String, ratingsOfVideo: Array[Double]) extends DenseVector[Double](ratingsOfVideo) with VideoVector with Serializable {
    override def getVideoName() = videoName

    override def toMLlibVector() = new org.apache.spark.mllib.linalg.DenseVector(ratingsOfVideo)
}

object Classification {
    
    //关键字个数
    private var kword = 20
    private val tabWeight = 5d
    private val nameWeight = 3d
    private val natureFilterList = List("ad","e","z","q","t","b","vd","s","v","m","d","y"
    ,"p","r","c","l","u","f","o")
    private val seriesClassList = scala.collection.mutable.Map[event_serie,List[(String,Int)]]()
    private val filterWords = List("-revise","转播","重播")
    private val simSeriesByClassList = scala.collection.mutable.Map[event_serie,List[(event_serie)]]()
    private val videoTabMap = scala.collection.mutable.Map[(String,String),Array[String]]()
    private val tabList = ListBuffer[(String,Double)]()
    private var tabWightList = List[(String,Double)]()
    private var eventVector = Array[VideoVector]()
    private var eventVecStr = Array[(String, Array[String])]()
   // private var SeriesVectorS = 
   // private val videoTab1 = RDD[(String,Array[String])]()
   
    implicit def EventToVideo(eit: event_serie) = video_serie(eit.series_id,eit.series_name,eit.service_id,5,"",
        1,eit.actors,eit.director,"内地","普通话",eit.description,1,"",eit.f_edit_time,eit.f_tab,80,"")
    
    implicit def cmp: Ordering[(Double, VideoVector)] = Ordering.by[(Double, VideoVector), Double](_._1)
    
    def tabVector(tabs:Array[String])={
          val feature = tabList.map(x=>if(tabs.contains(x._1)) x._2 else 0.0).toArray
          MyLogger.debug("feature:"+feature)
          feature
    }
    def Init(sc:SparkContext)={ 
        val videoAttrs = ListBuffer[video_serie]()
        val eventinfo = Live.getEventSeriesFullInfo
        val videoinfo = videoSeries.getVideoSeries
        val kwc = new KeyWordComputer(kword);
        
        eventinfo.foreach(video=>{
            val result = kwc.computeArticleTfidf(if(video.description == null) "" else video.description).map(x=>x.getName).toList 
            val tab = RTool.gettabList(if(video.f_tab == null) "" else video.f_tab).map(x=>(x,tabWeight))
            val name = kwc.computeArticleTfidf(if(video.series_name == null)"" else video.series_name ).map(x=>x.getName).toList
            val _video:video_serie = video
            tabList ++= (result.map(x=>(x,1D)) ++ tab ++ name.map(x=>(x,1D)) ++ List(video.series_name).map(x=>(x,nameWeight)) )
            videoTabMap += (((_video.series_name,_video.actors) -> result.toArray))
            videoAttrs += video
        })
        videoinfo.foreach(video=>{
            val result = kwc.computeArticleTfidf(if(video.description == null) "" else video.description).map(x=>x.getName).toList
            val tab = RTool.gettabList(if(video.f_tab == null) "" else video.f_tab).map(x=>(x,tabWeight))
            val name = kwc.computeArticleTfidf(if(video.series_name == null)"" else video.series_name ).map(x=>x.getName).toList
            val _video:video_serie = video
            tabList ++= (result.map(x=>(x,1D)) ++ tab ++ name.map(x=>(x,1D)) ++ List(video.series_name).map(x=>(x,nameWeight)) )
            videoTabMap += (((_video.series_name,_video.actors) -> result.toArray))
        })
        videoAttrs ++= videoinfo
        MyLogger.debug(s"videoAttrs size="+videoAttrs.size)
        val videoRDD = sc.makeRDD(videoAttrs)
        
        
        
        MyLogger.debug(s"tablist size=${tabList.size}")
        tabWightList = tabList.groupBy(x=>x._1).map(x=>x._2.reduce((s,t)=>(s._1,s._2 + t._2))).toArray.sortWith((s,t)=>s._2 > t._2).toList
        tabList.distinct
        MyLogger.debug(s"tablist distinct size=${tabList.size}")
        //calcFeatureMatrix(sc,videoTabMap.map(x=>tabVector(x._2)).toArray)
        /*val tab = videoTabMap.map(x=>x._1._1->x._2).toList
        
        eventVecStr = eventinfo.map(x=>(x.series_name,videoTabMap((x.series_name,x.actors)))).toArray
        val result1 = eventVecStr.map(x=>x._1->getNearestNeighbors(x))
        MyLogger.debug(s"eventVecStr result1:")
        result1.foreach(x=>{
            MyLogger.debug(s"${x._1}....")
            x._2.foreach(t=>MyLogger.debug(s"vec ${t._2},${t._1}"))
        })
        
        eventVector = eventinfo.map(x=>new VideoDenseVector(x.series_name,tabVector(videoTabMap((x.series_name,x.actors))))).toArray
        val eventVectorRDD = sc.makeRDD(eventVector)
        //to fix java.lang.IllegalArgumentException: Comparison method violates its general contract
        
        System.setProperty("java.util.Arrays.useLegacyMergeSort", "true");  
        val result = eventVector.map(x=>x->getNearestNeighbors(x))
        
        MyLogger.debug(s"eventVector result:")
        result.foreach(x=>{
            MyLogger.debug(s"${x._1.getVideoName()}....")
            x._2.foreach(t=>MyLogger.debug(s"vec ${t._2.getVideoName()},${t._1}|"))
        })*/
        MyLogger.debug("tabWightList result:")
        tabWightList.foreach(x=>MyLogger.debug(x.toString()))
        
    }
    def calcFeatureMatrix(sc:SparkContext,ma:Array[Array[Double]])={
        val rdd1 = sc.parallelize(ma).map(f=>Vectors.dense(f))
        val rowMatirx = new RowMatrix(rdd1)
        //rowMatirx.
        //var coordinateMatrix:CoordinateMatrix= rowMatirx.columnSimilarities()
       // MyLogger.debug(coordinateMatrix.entries.collect())
    }
    def getNearestNeighbors(arr:(String, Array[String]))={
        val result = eventVecStr.map(str=>{
            val a = arr._2
            val b = arr._2
            val unions = a.toSet & b.toSet
            (str._1,unions.size)
        }).sortWith((s,t)=>s._2 > t._2).take(20)
        result
    }
    def getNearestNeighbors(v:VideoVector)={
        
        val vecDistances = eventVector.map(x=> (CosineDistance.getDistance(v, x),x))
        var result = Array[(Double, VideoVector)]()
        try{
          result = vecDistances.sortWith((s,t)=>(s._1 > t._1)).take(20)
        }catch{
          case e:Exception=>{
            MyLogger.debug(s"${v.getVideoName()},,,"+vecDistances)
            e.printStackTrace()
          }
        }
        result
    }
     
    def getNearestNeighbors(rddVideo:List[VideoVector],v:VideoVector)={
        
        val vecDistances = rddVideo.map(x=> (CosineDistance.getDistance(v, x),x))
        var result = List[(Double, VideoVector)]()
        try{
          result = vecDistances.sortWith((s,t)=>(s._1 > t._1)).take(20)
        }catch{
          case e:Exception=>{
            MyLogger.debug(s"${v.getVideoName()},,,"+vecDistances)
            e.printStackTrace()
          }
        }
        result
    }
    //过滤一些没用的词汇
    private def wordFilter(word:String):Boolean={
        return !filterWords.contains(word)
    }  
    
    private def CalcSimBytab()={
        val tabShowList = ListBuffer[(event_serie,String)]()
        seriesClassList.foreach(x=>{
              x._2.foreach(y=>{
                  tabShowList += ((x._1,y._1))
              })
        })
        simSeriesByClassList
    }
    def getvideoTabMap()={
        videoTabMap
    }
    def getKextractKeyWord(video_info:Tuple2[String,String],kword:Int)={
        var result = Array("")
        
        if(videoTabMap.contains(video_info)){
            result = videoTabMap.getOrElse(video_info, null)
        }
        result
    }
    
    def getKextractKeyWord(_title:String,_con:String)={
        val title = if(_title == null) "" else _title
        val con = if(_con == null) "" else _con
        
        val kwc = new KeyWordComputer(kword);
        val result = kwc.computeArticleTfidf(title, con).map(x=>x.getName)
        val recognition = NatureRecognition.recognition(result, 0)
        val _re = recognition.map(x=>(x.getName,x.getNatureStr)).filter(x=> !natureFilterList.contains(x._2)).map(_._1).toList

        _re
    }

}