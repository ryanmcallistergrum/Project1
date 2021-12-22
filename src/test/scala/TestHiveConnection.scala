import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should
import org.apache.spark.sql.functions.col


class TestHiveConnection extends AnyFlatSpec with should.Matchers {
  object Test extends HiveConnection {
    var spark : SparkSession = null;

    def apply() : Unit = {
      spark = connect();
    }
    def executeDML(sql : String) : Unit = {
      super.executeDML(spark, sql);
    }
    def executeQuery(sql : String) : DataFrame = {
      super.executeQuery(spark, sql);
    }
    def showQuery(sql : String) : Unit = {
      super.showQuery(spark, sql);
    }
    def unapply() : Unit = {
      disconnect(spark);
    }
  }

  "connect()" should "successfully connect to the Hive" in {
    Test.apply();
    assert(Test.spark != null);
  }

  "executeQuery(SparkSession, String)" should "return a DataFrame when fetching all databases" in {
    Test.apply();
    val df : DataFrame = Test.executeQuery("show databases");
    assert(df != null);
  }

  it should "contain at least one record, default" in {
    Test.apply();
    val df: DataFrame = Test.executeQuery("show databases");
    assert(!df.isEmpty);
  }

  "executeDML(SparkSession, String)" should "execute successfully and modify the database" in {
    Test.apply();

    // First create a test database if it does not exist.
    Test.executeDML("create database if not exists test");
    var df: DataFrame = Test.executeQuery("show databases");
    assert(!df.filter(col("databaseName").contains("test")).isEmpty);

    // Now delete the test database.
    Test.executeDML("drop database test");
    df = Test.executeQuery("show databases");
    assert(df.filter(col("databaseName").contains("test")).isEmpty);
  }

  "showQuery(SparkSession, String)" should "output its results to standard output" in {
    Test.apply();
    Test.showQuery("show databases");
    assert(true);
  }

  "disconnect(SparkSession)" should "close an open SparkSession" in {
    Test.apply();
    assert(!Test.spark.sparkContext.isStopped);
    Test.unapply();
    assert(Test.spark.sparkContext.isStopped);
  }
}