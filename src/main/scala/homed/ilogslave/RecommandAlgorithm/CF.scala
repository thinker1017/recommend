package homed.ilogslave.recommendAlgorithm
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.SparkContext
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

//Collaborative filtering

/*
     * 5: Must see 
     * 4: Will enjoy 
     * 3: It's okay 
     * 2: Fairly bad 
     * 1: Awful 
*/   
 //todo ,负反馈处理，基于内容推荐
//mysql db 
// 用户直播节目推荐l_live_recommend
// 根据节目相似性推荐l_item_recommend
// 过去节目与未来节目关联  l_show_relate

case class seriesUser(
  series_id:Long,
  user_id:Long
)

object CF {
    private val hotPunish = 0.5
    
    private val hotShow = ListBuffer[event_serie]()
   
    //def 
    implicit def EventToVideo(eit: event_serie) = video_serie(eit.series_id,eit.series_name,eit.service_id,5,"",
        1,eit.actors,eit.director,"内地","普通话",eit.description,1,"",eit.f_edit_time,eit.f_tab,80,"")
    private def calcsimilarity[T](arr:Array[_])={
        arr match{
          case Array(seriesUser)=>{
            //calcSimseriesUser(arr)
          }
        }
    } 
    def calcSimseriesUser(sc:SparkContext,arr:Array[seriesUser])={
        var SimSeries = scala.collection.mutable.Map[Long,List[(Long,Double)]]()
        var Simmatrix = Map[(Long,Long),Double]()
        val UserSeriesMap = sc.makeRDD(arr).map(x=>x.user_id->x).aggregateByKey(ListBuffer[seriesUser]())((u, v) => u+=v, (u1, u2) => u1 ++ u2).mapValues(a=>a.toList).collectAsMap
        val SeriesUserMap = sc.makeRDD(arr).map(x=>x.series_id->x).aggregateByKey(ListBuffer[seriesUser]())((u, v) => u+=v, (u1, u2) => u1 ++ u2).mapValues(a=>a.toList).collectAsMap
        val activeSeries =  arr.map(x=>x.series_id).distinct
        //activeSeries.foreach(x=>MyLogger.debug(x.toString()))
        activeSeries.foreach(x=>{
            var hsa = new ListBuffer[(Long,Double)]()
            activeSeries.foreach(y=>{
                if(x != y && !SimSeries.contains(x)){
                     val usera = SeriesUserMap.getOrElse(x, null).map(x=>x.user_id)
                     val userb = SeriesUserMap.getOrElse(y, null).map(x=>x.user_id)
                     val userjoin = usera.toSet & userb.toSet
                      //热门惩罚
                     //MyLogger.debug(s"$usera,$userb,$userjoin")
                     val chu = math.pow(usera.size,1-hotPunish)*math.pow(userb.size,hotPunish)
                     if(userjoin.size !=0){
                        val sim = userjoin.size/chu
                        hsa += ((y,sim))
                        Simmatrix += ((x,y)->sim)
                        Simmatrix += ((y,x)->sim)
                      }
                }
            })
            val simItem = hsa.toList.sortWith((s,t)=>s._2 > t._2)
            SimSeries += (x -> simItem)
        })
        
        SimSeries
    }
    
}