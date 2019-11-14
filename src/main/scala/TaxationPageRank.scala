import scala.util.{Try, Success, Failure}
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import java.lang.Double
import org.apache.spark.sql.functions._

object TaxationPageRank {
    def main(args: Array[String]){
    //Titles = args[0] ("hdfs://honolulu:30541/PA3Input/Titles")
    //Links = args[1] ("hdfs://honolulu:30541/PA3Input/Links")
    //Output = args[2] ("hdfs://honolulu:30541/PA3Top10Taxation/")
    
        //Create the spark session
        val spark = SparkSession.builder.appName("PA3 - Taxation Page Rank").getOrCreate()
        val sc = SparkContext.getOrCreate()
        import spark.implicits._
        
        //Read in data
        val linkData = spark.read.textFile(args(1)).rdd
        val titles = sc.textFile(args(0)).zipWithIndex().mapValues(x => x+1).map(_.swap)
        
        //Change link data into the right format <from, Array(to, to, to....)>
        val links = linkData.map(x => (x.split(": ")(0), x.split(": ")(1).split(" +")))
        
        //Create v0
        val linkCount = links.count()
        var v = links.mapValues(v => 1.0 / linkCount)
        
        //Iterate 25 times
        for (i <- 1 to 25){
            //Perform M x V
            val m = links.join(v).values.flatMap{ case(urls, rank) =>
                val size = urls.size
                urls.map(url => (url, rank / size))
            }
            v = m.reduceByKey(_ + _).mapValues((0.15 / linkCount) + 0.85 * _)
        }

        //Get top10 page ranks
        val top10V = v.sortBy(_._2, false).map{case(myID, rank) => (myID.toLong, rank)}.take(10)
        val top10Indicies = sc.parallelize(top10V).map(x => x._1).collect()

        //Get titles for top10 page ranks
        val smallTitles = titles.filter{case(id, name) => top10Indicies.contains(id)}

        //Join            
        val joinWithTitles = sc.parallelize(top10V, 1).join(smallTitles).map{ case (id, (rank, name)) => (id, name, rank) }
        val sortedFinal = joinWithTitles.sortBy(_._3, false, 1).coalesce(1)

        sortedFinal.saveAsTextFile(args(2))  
    }

}
