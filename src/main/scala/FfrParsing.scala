/* FfrParsing.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object FfrParsing {
  def main(args: Array[String]) {
    val logFile = "file:///home/vwaj/upstart/local_data/sample_sanitized_ffrs"
    val conf = new SparkConf().setAppName("FFR Parsing")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2).cache()
    val numProspers = logData.filter(line => line.contains("PROSPER")).count()
    val numLendingClubs = logData.filter(line => line.contains("LENDING CLUB")).count()
    println("Lines with Prosper: %s, Lines with Lending Club: %s".format(numProspers, numLendingClubs))
  }
}
