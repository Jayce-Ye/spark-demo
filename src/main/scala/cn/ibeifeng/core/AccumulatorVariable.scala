package cn.ibeifeng.core

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * @author Administrator
 */
object AccumulatorVariable {
  
  def main(args: Array[String]) {
    val conf = new SparkConf()
        .setAppName("AccumulatorVariable")  
        .setMaster("local")  
    val sc = new SparkContext(conf)
    
    val sum = sc.longAccumulator("My Accumulator")
    
    val numberArray = Array(1, 2, 3, 4, 5) 
    val numbers = sc.parallelize(numberArray, 1)  
    numbers.foreach(x => sum.add(x))
    
    println(sum) 
  }
  
}