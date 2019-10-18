import java.io._
import scala.io.Source
import scala.math.random
import org.apache.spark._
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

//https://vincentarelbundock.github.io/Rdatasets/doc/DAAG/carprice.html

//case class Person(year: String, sex: String, education: String, vocabulary: String)
case class CarType(carType: String, price: String)
object Vocabulary {

  def FILENAME = "carprice"
  def K = "carType"
  def V = "price"
  def TIMES = 50

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("CarPrice").master("local[*]").getOrCreate()
    val csv = spark.sparkContext.textFile("carprice.csv")
    spark.sparkContext.setLogLevel("WARN")
    
    import spark.implicits._
    
    val headerAndRows = csv.map(line =>line.split(",").map(_.trim))
    val header = headerAndRows.first
    val mtcdata = headerAndRows.filter(_(0) != header(0))
    
    println("DATA ROWS: " + mtcdata.count())
    
    val persons = mtcdata
      .map(p => CarType(p(0), p(2)))
      .toDF.cache()
        persons.printSchema
    persons.select(K,V).show()
    // Aggregate data after grouping by columns
    import org.apache.spark.sql.functions._
    println("Step 2: Select a categorical variable and a numeric variable and form the key-value pair and create a pairRDD called “population”")
    val rdd1 = persons.select(K,V).cache()
    rdd1.show(10)

    println("Step 3. Compute the mean mpg and variance for each category")
    val mean_variance  = rdd1.groupBy(K).agg(avg(V) as "Mean",variance(V) as "Variance", stddev(V) as "Standard Deviation").show()

    println("Step 4: Create the sample for bootstrapping. All you need to do is take 25% of the population without replacement.")
    val rdd2 = rdd1.sample(false,.25)
    rdd2.show()
    println("In " + rdd2.count() +" rows")

    var result = rdd2.sample(true,1).groupBy(K).agg(avg(V) as "Mean",variance(V) as "Variance",stddev(V) as "Standard Deviation").cache()
    println("Step 5: first resample")
    result.show()
    var lastSample = result
    for(i<-2 to TIMES){
      var rdd3 = rdd2.sample(true,1).groupBy(K).agg(avg(V) as "Mean", variance(V) as "Variance",stddev(V) as "Standard Deviation")
      //lastSample is for demonstration only
      lastSample = rdd3
      if (i == TIMES)
        println("Step 5: last resample")
      lastSample.show()
      result = rdd3.union(result).cache()
      result = result.groupBy(K).agg(sum("Mean") as "Mean",sum("Variance") as "Variance",sum("Standard Deviation") as "Standard Deviation")

    }

    println("Step 5: sum mean, variance, stdd")
    result.show()

    println("Step 6: Divide each quantity by "+TIMES+" to get the average and display the result")
    val res = result.groupBy(K).agg(
      format_number(sum("Mean")/TIMES,2) as "Mean",
      format_number(sum("Variance")/TIMES,2) as "Variance",
      format_number(sum("Standard Deviation")/TIMES,2) as "Standard Deviation"
    )
    res.show()
  }
}
