import scala.util.{Try, Success, Failure}
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import java.lang.Double
import org.apache.spark.sql.functions._

object WikiBomb {
    def main(args: Array[String]){
    //Titles = args[0] ("hdfs://honolulu:30541/PA3Input/Titles")
    //Links = args[1] ("hdfs://honolulu:30541/PA3Input/Links")
    //Output = args[2] ("hdfs://honolulu:30541/PA3Top10Bomb/")
     
     //Create the spark context and session
        val spark = SparkSession.builder.appName("Wiki Bomb").getOrCreate()
        val sc = SparkContext.getOrCreate()
        
      //  Read in data
        val titles = sc.textFile(args(0)).zipWithIndex().mapValues(x => x+1).map(_.swap)
        val linkData = spark.read.textFile(args(1)).rdd
        val links = linkData.map(x => (x.split(": ")(0), x.split(": ")(1).split(" +")))
        val RMNP = titles.filter(x => x._2 == "Rocky_Mountain_National_Park").collect()(0)

      //  Regex for surfing
        val regex = ".*[Ss][Uu][Rr][Ff][Ii][Nn][Gg].*".r
        
      //  Filter out titles that contain "surfing"        
        val surfingTitles = titles.filter( x => regex.pattern.matcher(x._2).matches)
        val surfingIndices = surfingTitles.map(x => x._1).collect()
        
      //  Create new web-graph
        val initialWebGraph = links.filter{case(k,v) => (surfingIndices.contains(k.toInt))}.cache()
        
      //  Add Rocky Mountain to titles
        val rddOfRockyTitle = sc.parallelize(Seq(RMNP._1, RMNP._2)).collect{case(x : Long, y : String) => (x,y)}
        val titlesWithRocky = surfingTitles.union(rddOfRockyTitle)
        
      //  Add Rocky Mountain to web graph
        val rddOfRocky = spark.sparkContext.parallelize(Seq((RMNP.toString, Array[String]())))
        val webGraphWithRocky = initialWebGraph.union(rddOfRocky)
        
      //  Have all pages point to Rocky Mountain
        val finalWebGraph = webGraphWithRocky.map{case(myID : String, otherIDs : Array[String]) => (myID, otherIDs :+ RMNP._1.toString)}
        
     //   Run page rank
     //   Create v0
        val linkCount = finalWebGraph.count
        var v = links.mapValues(v => 1.0 / linkCount)
        
      //  Iterate 25 times
        for (i <- 1 to 25){
      //      Perform M x V
            val m = finalWebGraph.join(v).values.flatMap{ case(urls, rank) =>
                val size = urls.size
                urls.map(url => (url, rank / size))
            }
            v = m.reduceByKey(_ + _)
        }
        
        //Gets top 10 ranks and article ids 
        val sorted = v.sortBy(_._2, false, 1).map{case(myID, rank) => (myID.toLong, rank)}.take(10)
        val sortedIndicies = sc.parallelize(sorted, 1).map(x => x._1).collect()
        
        //Creates a smaller array with titles that match the top 10 ranks
        val smallTitles = titles.filter{case(id, name) => sortedIndicies.contains(id)}
        
        //Join top10 sorted and small titles and reformat
        val joinWithTitles = sc.parallelize(sorted,1).join(smallTitles).map{ case (id, (rank, name)) => (id, name, rank)}
        val top10 = joinWithTitles.sortBy(_._3, false, 1).coalesce(1)

        top10.saveAsTextFile(args(2))
    }
}
