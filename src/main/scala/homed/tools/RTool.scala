package homed.tools
import scala.collection.mutable.ListBuffer
import org.apache.spark.rdd.RDD
import homed.config.ReConfig._
import java.text.SimpleDateFormat
import java.util.Calendar
import homed.ilogslave.recommendAlgorithm._
import scala.math._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.util.regex._

import org.apache.log4j.BasicConfigurator  
import org.apache.log4j.Logger
import org.apache.log4j.PropertyConfigurator 
import org.apache.log4j.xml.DOMConfigurator


/**
 * @author lei
 */
object MyLogger{
     
     val log4j = Logger.getLogger("com.recommend.live")
     PropertyConfigurator.configure("./log4j.properties") 
     def info(message: => String){
         log4j.info(message)
     }
     def debug(message: => String){
         log4j.debug(message)
     }
     def warn(message: => String){
         log4j.warn(message)
     }
     def error(message: => String){
         log4j.error(message)
     }
} 

object RTool {
    private val editMaxLen = 100
    
    private val seriesFormat = List("剧场","动画","乐园","集锦","电视剧","连续剧","好剧连连看","连播","卡通","劇場","合家欢","邦锦梅朵","重播"
        ,"午间泡泡堂")
    private val calendar = Calendar.getInstance()
    private val ymdFormat = new SimpleDateFormat("yyyy-MM-dd")
    private val fs = FileSystem.get(new Configuration())
    //格式化直播回看节目名称
    private val jishuMatch1 = "([0-9]+)-([0-9]+)".r
    private val jishuMatch2 = "([0-9]+)/([0-9]+)".r
    private val jishuMatch3 = " ([0-9]+)".r
    //格式化直播回看节目名称
    private val SpecialregEx="[`~!@#$%^&*+=|{}';',\\[\\]《》.<>/?~！@#￥%……&*（）——+|{}【】‘；：”“’。，、？ⅠⅡⅢⅣⅤⅥⅦⅧⅨⅩⅰⅱⅲⅳⅴⅵⅶⅷⅸⅹ⑴⑵⑶⑷⑸⑹⑺⑻⑼⑽⑾⑿⒀⒁⒂⒃⒄⒅⒆⒇㈠㈡㈢㈣㈤㈥㈦㈧㈨㈩⒈⒉⒊⒋⒌⒍⒎⒏⒐⒑]"
    private val SpecialPattern = Pattern.compile(SpecialregEx)
    
     
    //格式化直播回看节目名称
    def formatShowName(str:String)={
         val _str = if(str == null) "" else str
         val str1 = _str.replaceAll("-revise", "").replaceAll("\\([0-9]+\\)|\\(精编版\\)|\\(下\\)|\\(非洲版\\)|\\(美洲版\\)|\\((亚洲版)\\)|\\(英\\)|\\(上\\)|\\(首播\\)|\\(复播\\)|\\(俄\\)|\\(西\\)|\\(法\\)|\\(阿\\)|\\(藏语\\)|\\(汉语\\)|\\(重播\\)|\\(双语\\)|\\{[0-9]+}|\\[[0-9]+]|（[0-9]+）", "").replaceAll("精编版|精编|预告|重播|首播|精选", "")
         
         val _str1 = if(( str1.contains("前情提要") || str1.contains("中国创造")||str1.contains("剧场") || str1.contains("酷片酷映") || str1.contains("精彩1刻")|| str1.contains("合家欢")||str1.contains("电视剧") || str1.contains("电影")||str1.contains("集锦") || str1.contains("动画") || str1.contains("连播") )&&str1.contains(":")) str1.split(":",2)(1) else str1
         
         val replace1 = jishuMatch1.findAllIn(_str1).toArray.mkString("")
         val replace2 = jishuMatch2.findAllIn(_str1).toArray.mkString("")
         val _str2 = _str1.replaceAll(replace1, "").replaceAll(replace2, "")
        // val replace3 = jishuMatch3.findAllIn(_str1).toArray.mkString("")
         SpecialPattern.matcher(_str2).replaceAll("").replaceAll("\\(.*\\)", "").replaceAll("第[0-9]*集", "").replaceAll("\\(*\\)", "").replaceAll("\\.|\\·", "").replaceAll("[0-9]*$", "").trim()
        
    }
    def getSavingFileName(str:String)={
        var saveFile = ""
        saveFile = getSaveFileName(str, false)
        if(fs.exists(new Path(saveFile))){
            val calendar = Calendar.getInstance()
            val ymdFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss")
            saveFile += ymdFormat.format(calendar.getTime)
            MyLogger.debug(s"has file ,create new file to save="+saveFile)
        }
        saveFile
    }
    def getSaveFileName(ftype:String,saved:Boolean)={
        var dateStr = ""
        var savedFile = ""
        var index = ""
        val calendar = Calendar.getInstance()
        val ymdFormat = new SimpleDateFormat("yyyy-MM-dd")
        if(saved){
           calendar.add(Calendar.DAY_OF_YEAR, -1)
           dateStr = if(BACKUPFILEDATA == "00-00-00") ymdFormat.format(calendar.getTime) else BACKUPFILEDATA
           index = SAVEDDATAINDEX
        }else{
           dateStr = if(SAVINGFILEDATA == "00-00-00") ymdFormat.format(calendar.getTime) else SAVINGFILEDATA
           index = SAVINGDATAINDEX
        }
        ftype match{
            //剧集数据补充，保存到hdfs
            case "videoDescription" =>{
               savedFile = ilogslaveRddSavePrefix+dateStr+"/"+"videoDescription"+".data"+index
            }
            case "eventSeries" =>{
               savedFile = ilogslaveRddSavePrefix+dateStr+"/"+"eventSeries"+".data"+index
            }
            case "feature" =>{
               savedFile = ilogslaveRddSavePrefix+dateStr+"/"+"feature"+".data"+index
            }
            case "disSim" =>{
               savedFile = ilogslaveRddSavePrefix+dateStr+"/"+"disSim"+".data"+index
            }
            case "traitDes" =>{
               savedFile = ilogslaveRddSavePrefix+dateStr+"/"+"traitDes"+".data"+index
            }
            case "liveHit" =>{
               savedFile = ilogslaveRddSavePrefix+dateStr+"/"+"liveHit"+".data"+index
            }
            case "featureScore" =>{
               savedFile = ilogslaveRddSavePrefix+dateStr+"/"+"featureScore"+".data"+index
            }
            
        }
        
        savedFile
    }
    def livNameformat(str:String)={
         //去掉干扰项
        val ss1 = str.replaceAll("-revise", "").replaceAll("精编", "").replaceAll("[(|0-9)]", "").split(":")
       
        def hasformatstr(s:String):Boolean={
            for(ff <- seriesFormat){
                if(s.contains(ff))return true
            }
            return false
        }
        val s1 = if(ss1.size >= 2 && hasformatstr(ss1(0))) ss1(1) else ss1(0)
        s1
    }
    def WordsDistance(str1:String,str2:String):Double={
        val words1 = Classification.getKextractKeyWord(str1,"")
        val words2 = Classification.getKextractKeyWord(str2,"")
        val k = words1.toSet & words2.toSet
        return k.size / (1.0 * max(words1.length,words2.length))
    }
   //编辑距离
    def editDistance(str1:String,str2:String):Tuple2[Int,Int]={
        
        val edis :Array[Array[Int]] = new Array(editMaxLen)
        for (k <-0 until editMaxLen ) {
          edis(k) = new Array[Int](editMaxLen)
        }
        val s1 = livNameformat(str1)
        val s2 = livNameformat(str2)
        
        val l_str1 = s1.length()
        val l_str2 = s2.length()
        // 记录相同字符,在某个矩阵位置值的增量,不是0就是1
        var temp = 0; 
        if (l_str1 == 0) return (l_str2,l_str2)
        if (l_str2 == 0) return (l_str1,l_str1)

        for (i <- 0 to l_str1) edis(i)(0) = i
        for (i <- 0 to l_str2) edis(0)(i) = i

        for (i <- 1 to l_str1) { 
            val ch1 = s1.charAt(i - 1)
            for (j <- 1 to l_str2) {
                val ch2 = s2.charAt(j - 1)
                if (ch1 == ch2 || ch1.isDigit) temp = 0 else temp = 1
                edis(i)(j) =  Math.min(edis(i - 1)(j) + 1, Math.min(edis(i)(j - 1) + 1, edis(i - 1)(j - 1)+ temp))
            }
        }
        (edis(l_str1)(l_str2),if (s1.length > s2.length) s1.length else s2.length)
        
    }
    //根据字符判断相似
    def calcSimilar(s1:String,s2:String)={
         val distance = editDistance(s1,s2)
         val similarity = (1 - distance._1.toDouble / distance._2)
         similarity
    }
    //根据词判断相似
    def calcSimilarByWords(s1:String,s2:String)={
         WordsDistance(s1,s2)
    }
    def getSimilarByShowName(localShow:Map[Long,(Long,String)],netShow:Map[Long,(Long,String)],log:String)={
      var showSimMap = scala.collection.Map[(Long,String),( Long, String,Double)]()  
      
      val localShowNum = localShow.size
      var findSim = 0
      var unSimList = ListBuffer[(Long,String)]()
      MyLogger.debug(s"${log},getSimilarByShowName size:${localShow.size},${netShow.size}")
      val stime = System.currentTimeMillis()
   
      
      localShow.foreach(x => {
            var maxSim = Tuple3[Long, Double, String](0, 0d, "0")
            for (p <- netShow ) {
              //if(x._2._1 == p._2._1){
                val distance = editDistance(x._2._2, p._2._2)
                val similarity = (1 - distance._1.toDouble / distance._2)
                maxSim = if (similarity > maxSim._2) (p._1, similarity, p._2._2) else maxSim
             // }
          }
          if (maxSim._1 != 0 && maxSim._2 > 0.5f) {
            MyLogger.debug(s"$log similar:${x._1},${x._2}------${maxSim._1},${maxSim._3},${maxSim._2}")
            showSimMap += ((x._1, x._2._2) -> (maxSim._1, maxSim._3, maxSim._2))
            showSimMap += ((maxSim._1, maxSim._3) -> (x._1, x._2._2, maxSim._2))
            findSim += 1
          } else {
            MyLogger.debug(s"$log not find sim ${x}")
            unSimList += ((x._1, x._2._2))
          }
      })
      val filterSimList = unSimList.map(x => x._2 -> x._1).toMap.map(x => (x._2, x._1))
      MyLogger.debug(s"$log find:$findSim,showNum:$localShowNum,searchNum:${filterSimList.size}")    
      MyLogger.debug("$log getSimilarByShowName run:"+(System.currentTimeMillis() - stime)/1000.0+"sec")
      showSimMap
    }
    
    val unlikechar = List("[，、]","、","。")
    def replaceUnLikeChar(content:String)={
        content.replaceAll("[，、]",",").replaceAll("。", ".")
    }
    
    def gettabListEx(tab:String)={
        val tlist = 
            
            if(tab.contains(",")) tab.split(",")
            else if(tab.contains(",")) tab.split(",")
            else if(tab.contains("|")) tab.replace("|",",").split(",")
            else if(tab.contains(" ")) tab.split(" ")
            else if(tab.contains("，")) tab.split("，")
            else if(tab.contains("、")) tab.split("、")
            //去掉诸如"新闻新闻"等合并错误的tag
            else if(tab.length() == 4 && (tab.substring(0,2) == tab.substring(2,4))) Array(tab.substring(0,2))
            else Array(tab)
        tlist.filter(x=>x.length() > 0 && x!="," && x!="," && x!="，" && x!="、" && x !=" ").map(x=>{
            if(x.length() == 4 && (x.substring(0,2) == x.substring(2,4)))x.substring(0,2)
            else x
        }) 
    }
    def gettabList(tab:String)={
        val t = gettabListEx(tab)
        t.map(x=>gettabListEx(x)).flatten
    }
}