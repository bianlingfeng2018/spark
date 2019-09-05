import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object HelloSparkCluster {
  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().setAppName("myHelloSparkCluster").setMaster("spark://node1:7077")
    val conf = new SparkConf().setAppName("myHelloSparkCluster").setMaster("local")

    val sc = new SparkContext(conf)
    //jar包位置 /Users/bianlingfeng/IdeaProjects/spark/target/spark-1.0-SNAPSHOT.jar
    sc.addJar("/Users/bianlingfeng/IdeaProjects/spark/target/spark-1.0-SNAPSHOT.jar")
    val data = Array("a", "a", "b", "c", "d d")
    val distData = sc.parallelize(data)
    distData.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).foreach(println)
  }
}
