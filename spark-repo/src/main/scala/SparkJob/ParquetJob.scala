package SparkJob

import SparkJob.Domain._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.lit

object ParquetJob extends DataJob[DataFrame, DataFrame] {

  override def read(
      params: SparkParams
  )(implicit spark: SparkSession) = {
    var dataReader = spark.read
    params.inOptions.toSeq.foreach { op =>
      dataReader = dataReader.option(op._1, op._2)
    }
    val inputDF = dataReader.parquet(params.inPath)
    inputDF
  }

  override def transform(
      data: DataFrame
  )(implicit spark: SparkSession, params: SparkParams) = {
    import spark.implicits._

    val outputDF = data.withColumn("source", lit("wcd"))
    
    SaveParameters(outputDF, params)
  }

  override def save(p: SaveParameters) {
    p.df.write
    .partitionBy(p.params.partitionColumn)
    .options(p.params.outOptions)
    .format(p.params.outFormat)
    .mode(p.params.saveMode)
    .save(p.params.outPath)      
  }
}