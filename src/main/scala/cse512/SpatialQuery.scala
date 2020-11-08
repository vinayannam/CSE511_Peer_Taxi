package cse512

import org.apache.spark.sql.SparkSession

object SpatialQuery extends App{

  // For implementation we are using interactive scala to simulate the working
  // of the function on a test data input.

  // USER DEFINED FUNCTION: ST_Contains

  def ST_Contains(queryRectangle:String, pointString:String): Boolean = {

    // Input: pointString:String, queryRectangle:String
    // Output: Boolean (true or false)
    // Definition: You first need to parse 
    // the pointString (e.g., "-88.331492,32.324142") and 
    // queryRectangle (e.g., "-155.940114,19.081331,-155.618917,19.5307") 
    // to a format that you are comfortable with. 
    // Then check whether the queryRectangle fully contains the point. 
    // Consider on-boundary point.

    // rx1, ry1, rx2, ry2: represents the rectangle
    // px, py: represents the point coordinate

    // Parsing the rectangle string
    val rectangle = queryRectangle.split(',')
    // getting the coordinates, trimming for validation and converting to Double
    val rx1 = rectangle(0).trim().toDouble
    val ry1 = rectangle(1).trim().toDouble
    val rx2 = rectangle(2).trim().toDouble
    val ry2 = rectangle(3).trim().toDouble

    // Parsing the point string
    val point = pointString.split(',')
    // getting the coordinates, trimming for validation and converting to Double
    val px = point(0).trim().toDouble
    val py = point(1).trim().toDouble

    if ((rx1-px)*(px-rx2) >= 0 && (ry1-py)*(py-ry2) >= 0){
      return true
    }else{
      return false
    }
  }

  // USER DEFINED FUNCTION: ST_Within

  def ST_Within(pointString1:String, pointString2:String, distance:Double): Boolean = {

    // Input: pointString1:String, pointString2:String, distance:Double
    // Output: Boolean (true or false)
    // Definition: You first need to parse 
    // the pointString1 (e.g., "-88.331492,32.324142") and 
    // pointString2 (e.g., "-88.331492,32.324142") 
    // to a format that you are comfortable with. 
    // Then check whether the two points are within the given distance. 
    // Consider on-boundary point. To simplify the problem, 
    // please assume all coordinates are on a planar space and calculate their Euclidean distance.

    // Parsing the first point string
    val point1 = pointString1.split(',')
    // getting the coordinates, trimming for validation and converting to Double
    val px1 = point1(0).trim().toDouble
    val py1 = point1(1).trim().toDouble

    // Parsing the second point string
    val point2 = pointString2.split(',')
    // getting the coordinates, trimming for validation and converting to Double
    val px2 = point2(0).trim().toDouble
    val py2 = point2(1).trim().toDouble

    // calculating euclidean distance using sqrt((x1-x2)^2+(y1-y2)^2)
    val x_diff = px1-px2
    val y_diff = py1-py2
    val x_diff_sqr = scala.math.pow(x_diff, 2)
    val y_diff_sqr = scala.math.pow(y_diff, 2)
    val ed = scala.math.pow(x_diff_sqr+y_diff_sqr, 0.5)

    if (ed <= distance){
      return true
    }else{
      return false
    }
  }

  def debug(count:Double) = {
    print("Count: "+count)
    print("\n")
  }


  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")


    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>(ST_Contains(queryRectangle, pointString)))

    val resultDf = spark.sql("select * from point where ST_Contains('"+arg2+"',point._c0)")
    resultDf.show()

    val count = resultDf.count()
    
    // for debug
    debug(count)

    return count
  }

  def runRangeJoinQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    rectangleDf.createOrReplaceTempView("rectangle")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>(ST_Contains(queryRectangle, pointString)))

    val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
    resultDf.show()
    
    val count = resultDf.count()
    
    // for debug
    debug(count)

    return count
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>(ST_Within(pointString1, pointString2, distance)))

    val resultDf = spark.sql("select * from point where ST_Within(point._c0,'"+arg2+"',"+arg3+")")
    resultDf.show()
    
    val count = resultDf.count()
    
    // for debug
    debug(count)

    return count
  }

  def runDistanceJoinQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point1")

    val pointDf2 = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    pointDf2.createOrReplaceTempView("point2")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>(ST_Within(pointString1, pointString2, distance)))
    val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, "+arg3+")")
    resultDf.show()
    
    val count = resultDf.count()
    
    // for debug
    debug(count)

    return count
  }
}
