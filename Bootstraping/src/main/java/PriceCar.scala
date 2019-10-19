import java.io._
import scala.io.Source
import scala.math.random
import org.apache.spark._
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

//https://vincentarelbundock.github.io/Rdatasets/doc/DAAG/carprice.html

case class CarType(carType: String, price: Double)
object CarPrice {
  def K = "carType"
  def V = "price"
  def TIMES = 10
  def SAMPLE_RATE = 0.75

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("CarPrice").master("local[*]").getOrCreate()
    val csv = spark.sparkContext.textFile("carprice.csv")
    spark.sparkContext.setLogLevel("WARN")
    
    import spark.implicits._
    
    val headerAndRows = csv.map(line =>line.split(",").map(_.trim))
    val header = headerAndRows.first
    val mtcdata = headerAndRows.filter(_(0) != header(0))
    
    println("DATA ROWS: " + mtcdata.count())
    
    val carTypes = mtcdata.map(p => CarType(p(0), p(2).toDouble)).toDF.cache()
        
    carTypes.select(K,V).show(48)
    
    // Aggregate data after grouping by columns
    import org.apache.spark.sql.functions._
    println("Step 2: Select a categorical variable and a numeric variable and form the key-value pair and create a pairRDD called “population”")
    val population = carTypes.select(K,V).cache()
    population.show()

    println("Step 3. Compute the mean price and variance for each car type and display")
    val mean_variance  = population.groupBy(K).agg(avg(V) as "Mean",variance(V) as "Variance").show()

    println("Step 4: Create the sample for bootstrapping, take 25% of the population without replacement.")
    val sampleRdd = population.sample(false,SAMPLE_RATE)
    sampleRdd.show()

    println("Step 5: Create a resampledData and compute the mean mpg and variance for each category")
    val firstResample = sampleRdd.sample(true,1).cache()
    var result = firstResample.groupBy(K).agg(avg(V) as "Mean", variance(V) as "Variance").cache()
    result.show()
    println("10%")
    for(i<-2 to TIMES){
      var resampledData = sampleRdd.sample(true,1).groupBy(K).agg(avg(V) as "Mean", variance(V) as "Variance")
      result = resampledData.union(result).cache()
      println((100/TIMES * i) + "%")
    }

    println("Step 5: sum mean, variance")
    result = result.groupBy(K).agg(sum("Mean") as "Mean",sum("Variance") as "Variance")
    result.show()

    println("Step 6: Divide each quantity by "+TIMES+" to get the average and display the result")
    val res = result.groupBy(K).agg(
      format_number(sum("Mean")/TIMES,2) as "Mean",
      format_number(sum("Variance")/TIMES,2) as "Variance"
    )

    res.show()
  }
}
