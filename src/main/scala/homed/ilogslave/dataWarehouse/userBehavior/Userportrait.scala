package homed.ilogslave.dataWarehouse.userBehavior
import homed.tools._
import org.apache.spark.SparkContext
import scala.collection.mutable.ListBuffer
import homed.ilogslave.dataWarehouse._
import homed.ilogslave.recommendAlgorithm._
import org.apache.spark.rdd.PairRDDFunctions
import breeze.linalg._

case class basicProtrait(
    DA:Long,
    account_name:String,
    sex:Int,
    birthday:java.sql.Date,
    age:Int
)

case class labelProtrait(
    label:String,
    labelWeight:Double,
    labelType:Int
)

case class Portrait(
    basicp:basicProtrait,
    babelp:Array[labelProtrait]
)


//用户行为
case class UserVideoBehavior(
    DA:Long,
    account_name:String, 
    //行为时间
    hitTime:Long,
    series:video_serie,
    //观看时长
    watchTime:Int
)

trait UserVector extends Vector[Double] with Serializable {
  def getUserID(): Int

  def toMLlibVector(): org.apache.spark.mllib.linalg.Vector

  override def toString(): String = {
    "UserID: " + getUserID() + " " + super.toString()
  }
}
        
object Userportrait {
  
    private val userVideoBehavior = ListBuffer[UserVideoBehavior]()
    private var userinfoMap = Map[Long,account_info]()
    private var userPortrait = ListBuffer[Portrait]()
    private var l_userRecommend = scala.collection.mutable.Map[Long,List[video_serie]]()
    private var protrait = Array[(basicProtrait, ListBuffer[labelProtrait])]()
   /* 
    private implicit def EventToVideo(eit: event_serie) = video_serie(eit.series_id,eit.series_name,eit.service_id,5,"",
        1,eit.actors,eit.director,"内地","普通话",eit.description,1,"",eit.f_edit_time,eit.f_tab,80,"")
    */
    def Init()={
        userVideoBehavior.clear()
        userinfoMap = userInfo.getUserInfoMap()
        
    }
    def putBehavior(hit:Array[user_Video_Hit])={
        userVideoBehavior ++= hit.map(x=>UserVideoBehavior(x.DA,"",x.hitTime,x.series,x.watchTime)).toList
    }
    
    def calcProtrait(sc:SparkContext)={
        val date = new java.sql.Date(new java.util.Date().getTime)
        
        val ulog = userVideoBehavior.map(x=>x.DA->x).sortWith((s,t)=>s._1 > t._1)
        ulog.foreach(x=>{
          val a = Classification.getKextractKeyWord((x._2.series.series_name,x._2.series.actors),20).mkString("|")
          MyLogger.debug(s"ulog ${x._1},${x._2.series.series_name},${a}")
        })
        
        val rddPortrait = sc.makeRDD(userVideoBehavior).map(x=>{
          MyLogger.debug(s"calc ${x}")
          val tabList = Classification.getKextractKeyWord((x.series.series_name,x.series.actors),20) ++ RTool.gettabList(x.series.actors) ++  RTool.gettabList(x.series.director)
          val aa = tabList.map(tab=>(tab,1.0,1)).groupBy(x=>x._1).map(x=>x._2.reduce((s,t)=>(s._1,s._2 + t._2,s._3))).toArray.sortWith((s,t)=>s._2 > t._2).map(x=>(labelProtrait(x._1,x._2,x._3))).toArray
          MyLogger.debug("calc result")
          aa.foreach(x=>MyLogger.debug(s"x ${x.label},${x.labelWeight}"))
          Portrait(basicProtrait(x.DA,x.account_name,0,date,0),aa)
        })
        rddPortrait.collect.foreach(x=>{
          MyLogger.debug(s"combine before ${x.basicp.DA}")
          val protrait = x.babelp.take(10)
          protrait.foreach(x=>MyLogger.debug(x.toString()))
        })
        
        val pRdd = new PairRDDFunctions(rddPortrait.map(x=>x.basicp->x.babelp))
        val combineRdd = pRdd.combineByKey(v => (new ListBuffer[labelProtrait]) ++= v, (c: ListBuffer[labelProtrait], v) => c ++= v, (c1: ListBuffer[labelProtrait], c2: ListBuffer[labelProtrait]) => 
          c1 ++ c2.groupBy(x=>x.label).map(x=>x._2.reduce((s,t)=>labelProtrait(s.label,s.labelWeight+t.labelWeight,s.labelType)))
        )
        
        protrait = combineRdd.collect
        MyLogger.debug("Protrait result:")
        protrait.foreach(x=>{
          MyLogger.debug(s"${x._1.DA},${x._1.account_name}")
          val protrait = x._2.sortWith((s,t)=>s.labelWeight > t.labelWeight).take(10)
          protrait.foreach(x=>MyLogger.debug(x.toString()))
        })
    }
    
    def ProtraitResult()={
      
        val eventSeriesList = Live.geteventSeries.toList
        val videoSeriesList = videoSeries.getVideoSeries().toList
        val videoTab = Classification.getvideoTabMap
        videoTab
    }
}