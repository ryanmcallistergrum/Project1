import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}


class HiveConnection {

  protected def connect() : SparkSession = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.spark-project").setLevel(Level.ERROR)
    Logger.getLogger("org").setLevel(Level.ERROR);
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    val spark = SparkSession
      .builder
      .appName("Project1")
      .config("spark.master", "local[*]")
      .enableHiveSupport()
      .getOrCreate();
    spark.sparkContext.setLogLevel("ERROR")
    return spark;
  }

  protected def executeDML(spark : SparkSession, sql : String) : Unit = {
    spark.sql(sql).queryExecution;
  }

  protected def executeQuery(spark : SparkSession, sql : String) : DataFrame = {
    return spark.sql(sql);
  }

  protected def showQuery(spark : SparkSession, sql : String) : Unit = {
    spark.sql(sql).show(Integer.MAX_VALUE, false);
  }

  protected def disconnect(spark : SparkSession) : Unit = {
    spark.close();
  }
}