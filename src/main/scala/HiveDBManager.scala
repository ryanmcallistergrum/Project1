import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import com.roundeights.hasher.Implicits._

import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


class HiveDBManager extends HiveConnection {
  protected def createDB() : Unit = {
    val spark : SparkSession = connect();
    executeDML(spark, "create database if not exists p1");
    createUsersCopy("p1.users");
    createQueriesCopy("p1.queries");
    createGamesCopy("p1.games");
    createReviewsCopy("p1.reviews");
    createArticlesCopy("p1.articles");

    executeDML(spark, s"insert into p1.users values (${getNextUserId()}, 'admin', '${"admin".sha512.hex}', true)");
  }

  private def createUsersCopy(table_name : String) : Unit = {
    executeDML(connect(),
      "create table if not exists " +
        s"$table_name(" +
          "user_id Int, " +
          "username String, " +
          "password String, " +
          "isAdmin Boolean " +
        ") " +
        "stored as orc"
    );
  }

  private def createQueriesCopy(table_name : String) : Unit = {
    executeDML(connect(),
      "create table if not exists " +
        s"$table_name(" +
          "query_id Int, " +
          "user_id Int, " +
          "query_name String, " +
          "query String " +
        ") " +
        "stored as orc"
    );
  }

  private def createGamesCopy(table_name : String) : Unit = {
    executeDML(connect(),
      "create table if not exists " +
        s"$table_name(" +
          "game_id BigInt, " +
          "name String, " +
          "release_date Timestamp, " +
          "deck String, " +
          "description String, " +
          "articles_api_url String, " +
          "reviews_api_url String, " +
          "avg_score Double, " +
          "article_count BigInt, " +
          "review_count BigInt, " +
          "genres Array<String>, " +
          "themes Array<String> " +
        ") " +
        "partitioned by (year String) " +
        "clustered by (genres, themes) " +
        "sorted by (release_date) " +
        "into 50 buckets " +
        "stored as orc"
    );
  }

  private def createArticlesCopy(table_name : String) : Unit = {
    executeDML(connect(),
      "create table if not exists " +
        s"$table_name(" +
          "article_id BigInt, " +
          "authors String, " +
          "title String, " +
          "deck String, " +
          "lede String, " +
          "body String, " +
          "publish_date Timestamp, " +
          "update_date Timestamp, " +
          "categories map<BigInt, String>, " +
          "game_id BigInt" +
        ") " +
        "partitioned by (year String) " +
        "clustered by (game_id) " +
        "sorted by (publish_date) " +
        "into 10000 buckets " +
        "stored as orc"
    );
  }

  private def createReviewsCopy(table_name : String) : Unit = {
    executeDML(connect(),
      "create table if not exists " +
        s"$table_name(" +
          "review_id BigInt, " +
          "authors String, " +
          "title String, " +
          "deck String, " +
          "lede String, " +
          "body String, " +
          "publish_date Timestamp, " +
          "update_date Timestamp, " +
          "score Double, " +
          "review_type String, " +
          "game_id BigInt" +
        ") " +
        "partitioned by (year String) " +
        "clustered by (review_type) " +
        "sorted by (publish_date) " +
        "into 3 buckets " +
        "stored as orc"
    );
  }

  def getNextUserId() : Int = {
    var result : Int = 1;
    val spark : SparkSession = connect();
    val df : DataFrame = executeQuery(spark, "select max(user_id) from p1.users");
    if (!df.isEmpty)
      if (!df.take(1)(0).isNullAt(0))
        result = df.take(1)(0).getInt(0) + 1

    return result;
  }

  protected def getUsers() : List[(Int, String, String, Boolean)] = {
    var result : ArrayBuffer[(Int, String, String, Boolean)] = ArrayBuffer();
    val spark : SparkSession = connect();
    val df : DataFrame = executeQuery(spark, "select user_id, username, password, isAdmin from p1.users order by user_id");
    if (!df.isEmpty)
      if (!df.take(1)(0).isNullAt(0))
        for (r : Row <- df.collect()) {
          val tuple : (Int, String, String, Boolean) = (r.getInt(0), r.getString(1), r.getString(2), r.getBoolean(3));
          result += tuple;
        };

    return result.toList;
  }

  def authenticate(username : String, password : String, isAdmin : Boolean) :  Int = {
    var result : Int = 0;
    val spark : SparkSession = connect();
    val df : DataFrame = executeQuery(spark, s"select user_id from p1.users where username = '$username' and password = '${password.sha512.hex}' and isAdmin = $isAdmin");
    if (!df.isEmpty)
      if (!df.take(1)(0).isNullAt(0))
        result = df.take(1)(0).getInt(0);

    return result;
  }

  def addUser(username : String, password : String) : Int = {
    val result : Int = getNextUserId();
    executeDML(connect(), s"insert into p1.users values ($result, '$username', '${password.sha512.hex}', false)");
    return result;
  }

  def updateUsername(user_id : Int, newUsername : String) : Boolean = {
      val spark : SparkSession = connect();
      val df : DataFrame = executeQuery(spark, s"select password, isAdmin from p1.users where user_id = $user_id");
      val passAdmin : (String, Boolean) = (df.take(1)(0).getString(0), df.take(1)(0).getBoolean(1));
      createUsersCopy("p1.usersTemp");
      executeDML(spark, s"insert into p1.usersTemp select * from p1.users where user_id != $user_id");
      executeDML(spark, "drop table p1.users");
      executeDML(spark, "alter table p1.usersTemp rename to users");
      executeDML(spark, s"insert into table p1.users values ($user_id, '$newUsername', '${passAdmin._1}', ${passAdmin._2})");
      return true;
  }

  def updatePassword(user_id : Int, oldPassword : String, newPassword : String) : Boolean = {
    val spark : SparkSession = connect();
    var df : DataFrame = executeQuery(spark, s"select * from p1.users where user_id = $user_id and password = '${oldPassword.sha512.hex}'");
    if (df.isEmpty)
      return false;
    else {
      df = executeQuery(spark, s"select username, isAdmin from p1.users where user_id = $user_id");
      val userAdmin : (String, Boolean) = (df.take(1)(0).getString(0), df.take(1)(0).getBoolean(1));
      createUsersCopy("p1.usersTemp");
      executeDML(spark, s"insert into p1.usersTemp select * from p1.users where user_id != $user_id");
      executeDML(spark, "drop table p1.users");
      executeDML(spark, "alter table p1.usersTemp rename to users");
      executeDML(spark, s"insert into table p1.users values ($user_id, '${userAdmin._1}', '${newPassword.sha512.hex}', ${userAdmin._2})");
      return true;
    }
  }

  def usernameExists(username : String) : Boolean = {
    val spark : SparkSession = connect();
    val df : DataFrame = executeQuery(spark, s"select username from p1.users where username = '$username'");
    return !df.isEmpty;
  }

  protected def isAdmin(user_id : Int) : Boolean = {
    val spark : SparkSession = connect();
    val df : DataFrame = executeQuery(spark, s"select isAdmin from p1.users where user_id = $user_id");
    return df.take(1)(0).getBoolean(0);
  }

  protected def setToAdmin(user_id : Int) : Unit = {
      val spark : SparkSession = connect();
      var df : DataFrame = executeQuery(spark, s"select username, password from p1.users where user_id = $user_id");
      val userPass : (String, String) = (df.take(1)(0).getString(0), df.take(1)(0).getString(1));
      createUsersCopy("p1.usersTemp");
      executeDML(spark, s"insert into p1.usersTemp select * from p1.users where user_id != $user_id");
      executeDML(spark, "drop table p1.users");
      executeDML(spark, "alter table p1.usersTemp rename to users");
      executeDML(spark, s"insert into table p1.users values ($user_id, '${userPass._1}', '${userPass._2}', true)");
  }

  protected def deleteUser(user_id : Int) : Unit = {
      val spark : SparkSession = connect();
      createUsersCopy("p1.usersTemp");
      executeDML(spark, s"insert into p1.usersTemp select * from p1.users where user_id != $user_id");
      executeDML(spark, "drop table p1.users");
      executeDML(spark, "alter table p1.usersTemp rename to users");
  }

  protected def getNextQueryId() : Int = {
    var result : Int = 1;
    val spark : SparkSession = connect();
    val df : DataFrame = executeQuery(spark, "select max(query_id) from p1.queries");
    if (!df.isEmpty)
      if (!df.take(1)(0).isNullAt(0))
        result = df.take(1)(0).getInt(0);

    return result;
  }

  def getQueries() : Map[Int, String] = {
    var result : Map[Int, String] = Map();
    val spark : SparkSession = connect();
    val df : DataFrame = executeQuery(spark, "select query_id, query_name from p1.queries order by query_id");
    if (!df.isEmpty)
      if (!df.take(1)(0).isNullAt(0))
        for (row : Row <- df.collect())
          result += (row.getInt(0) -> row.getString(1));
    return result;
  }

  def queryNameExists(query_name : String) : Boolean = {
    val queries : Map[Int, String] = getQueries();
    if (queries.isEmpty)
      return false;
    else
      return queries.values.find(q => q.equals(query_name)).get.nonEmpty;
  }

  def showQuery(query_id : Int) : Unit = {
    val spark : SparkSession = connect();
    val df : DataFrame = executeQuery(spark, s"select query from p1.queries where query_id = $query_id");
    if (!df.isEmpty)
      if (!df.take(1)(0).isNullAt(0))
        showQuery(spark, df.take(1)(0).getString(0));
  }

  protected def showQuery(query : String) : Unit = {
    showQuery(connect(), query);
  }

  protected def saveQuery(user_id : Int, query_name : String, query : String) : Unit = {
    executeDML(connect(), s"insert into p1.queries values (${getNextQueryId()}, $user_id, '$query_name', '$query')");
  }

  protected def deleteQuery(query_id : Int) : Unit = {
    val spark : SparkSession = connect();
    createQueriesCopy("p1.queriesTemp");
    executeDML(spark, s"insert into p1.queriesTemp select * from p1.queries where query_id != $query_id");
    executeDML(spark, "drop table p1.queries");
    executeDML(spark, "alter table p1.queriesTemp rename to queries");
  }

  def addGame(game_id : Long, name : String, release_date : LocalDateTime, deck : String, description : String, articles_api_url : String, reviews_api_url : String, avg_score : Double, article_count : Long, review_count : Long, genres : List[String], themes : List[String]) : Unit = {
      val genresString : String = "'" + genres.mkString("','") + "'";
      val themesString : String = "'" + themes.mkString("','") + "'";
      val dateTime : String = release_date.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
      executeDML(connect(),
        s"insert into p1.games partition(year='${release_date.getYear}') select " +
          s"${game_id}L, " +
          s"'$name', " +
          s"to_timestamp('$dateTime'), " +
          s"'$deck', " +
          s"'$description', " +
          s"'$articles_api_url', " +
          s"'$reviews_api_url', " +
          s"$avg_score, " +
          s"${article_count}L, " +
          s"${review_count}L, " +
          s"ARRAY($genresString), " +
          s"ARRAY($themesString)"
      );
  }

  def addGames(games : List[(Long, String, LocalDateTime, String, String, String, String, Double, Long, Long, List[String], List[String])]) : Unit = {
    for (game : (Long, String, LocalDateTime, String, String, String, String, Double, Long, Long, List[String], List[String]) <- games)
      addGame(
        game._1,
        game._2,
        game._3,
        game._4,
        game._5,
        game._6,
        game._7,
        game._8,
        game._9,
        game._10,
        game._11,
        game._12
      );
  }

  def gameExists(game_id : Long, name : String) : Boolean = {
    val game : (Long, String, LocalDateTime, String, String, String, String, Double, Long, Long, List[String], List[String]) = getGame(game_id);
    if (game == null)
      return false;
    else
      return game._2.equals(name);
  }

  def getGame(game_id : Long) : (Long, String, LocalDateTime, String, String, String, String, Double, Long, Long, List[String], List[String]) = {
    var result : (Long, String, LocalDateTime, String, String, String, String, Double, Long, Long, List[String], List[String]) = null;
    val df : DataFrame = executeQuery(connect(), s"select * from p1.games where game_id = ${game_id}L limit 1");
    if (!df.isEmpty) {
      val row : Row = df.take(1)(0);
      val genres : List[String] = row.get(10).asInstanceOf[mutable.WrappedArray[String]].toList;
      val themes : List[String] = row.get(11).asInstanceOf[mutable.WrappedArray[String]].toList;
      result = (
        row.getLong(0),
        row.getString(1),
        row.getTimestamp(2).toLocalDateTime,
        row.getString(3),
        row.getString(4),
        row.getString(5),
        row.getString(6),
        row.getDouble(7),
        row.getLong(8),
        row.getLong(9),
        genres,
        themes
      );
    }
    return result;
  }

  def getGame(game_name : String) : (Long, String, LocalDateTime, String, String, String, String, Double, Long, Long, List[String], List[String]) = {
    var result : (Long, String, LocalDateTime, String, String, String, String, Double, Long, Long, List[String], List[String]) = null;
    val df : DataFrame = executeQuery(connect(), s"select * from p1.games where name = '$game_name' limit 1");
    if (!df.isEmpty) {
      val row : Row = df.take(1)(0);
      val genres : List[String] = row.get(10).asInstanceOf[mutable.WrappedArray[String]].toList;
      val themes : List[String] = row.get(11).asInstanceOf[mutable.WrappedArray[String]].toList;
      result = (
        row.getLong(0),
        row.getString(1),
        row.getTimestamp(2).toLocalDateTime,
        row.getString(3),
        row.getString(4),
        row.getString(5),
        row.getString(6),
        row.getDouble(7),
        row.getLong(8),
        row.getLong(9),
        genres,
        themes
      );
    }
    return result;
  }

  def getGamesBetween(startDate : LocalDateTime, endDate : LocalDateTime) : List[(Long, String, LocalDateTime, String, String, String, String, Double, Long, Long, List[String], List[String])] = {
    var result : ArrayBuffer[(Long, String, LocalDateTime, String, String, String, String, Double, Long, Long, List[String], List[String])] = ArrayBuffer();
    val df : DataFrame = executeQuery(connect(), s"select * from p1.games where release_date between '${Timestamp.valueOf(startDate)}' and '${Timestamp.valueOf(endDate)}' order by game_id")
    if (!df.isEmpty)
      if (!df.take(1)(0).isNullAt(0))
        for(row : Row <- df.collect()) {
          val genres : List[String] = row.get(10).asInstanceOf[mutable.WrappedArray[String]].toList;
          val themes : List[String] = row.get(11).asInstanceOf[mutable.WrappedArray[String]].toList;
          val tuple : (Long, String, LocalDateTime, String, String, String, String, Double, Long, Long, List[String], List[String]) = (
            row.getLong(0),
            row.getString(1),
            row.getTimestamp(2).toLocalDateTime,
            row.getString(3),
            row.getString(4),
            row.getString(5),
            row.getString(6),
            row.getDouble(7),
            row.getLong(8),
            row.getLong(9),
            genres,
            themes
          );
          result += tuple;
        }

    return result.toList;
  }

  def getMaxGameIdBetween(startDate : LocalDateTime, endDate : LocalDateTime) : Long = {
    var result : Long = 0L;
    val df : DataFrame = executeQuery(connect(), s"select max(game_id) from p1.games where release_date between '${Timestamp.valueOf(startDate)}' and '${Timestamp.valueOf(endDate)}'");
    if (!df.isEmpty)
      if (!df.take(1)(0).isNullAt(0))
        result = df.take(1)(0).getLong(0);

    return result;
  }

  def getGameCountBetween(startDate : LocalDateTime, endDate : LocalDateTime) : Long = {
    var result : Long = 0L;
    val df : DataFrame = executeQuery(connect(), s"select count(*) from p1.games where release_date between '${Timestamp.valueOf(startDate)}' and '${Timestamp.valueOf(endDate)}'");
    if (!df.isEmpty)
      if (!df.take(1)(0).isNullAt(0))
        result = df.take(1)(0).getLong(0);

    return result;
  }

  def getLatestGames() : List[(Long, String, LocalDateTime, String, String, String, String, Double, Long, Long, List[String], List[String])] = {
    var results : ArrayBuffer[(Long, String, LocalDateTime, String, String, String, String, Double, Long, Long, List[String], List[String])] = ArrayBuffer();
    val df : DataFrame = executeQuery(connect(), "select * from p1.games where release_date = (select max(release_date) from p1.games)");
    if (!df.isEmpty)
      if (!df.take(1)(0).isNullAt(0)) {
        for(row : Row <- df.collect()) {
          val genres : List[String] = row.get(10).asInstanceOf[mutable.WrappedArray[String]].toList;
          val themes : List[String] = row.get(11).asInstanceOf[mutable.WrappedArray[String]].toList;
          val game : (Long, String, LocalDateTime, String, String, String, String, Double, Long, Long, List[String], List[String]) = (
            row.getLong(0),
            row.getString(1),
            row.getTimestamp(2).toLocalDateTime,
            row.getString(3),
            row.getString(4),
            row.getString(5),
            row.getString(6),
            row.getDouble(7),
            row.getLong(8),
            row.getLong(9),
            genres,
            themes
          );
            results += game;
        }
      };
    return results.toList;
  }

  def getGameIds() : List[Long] = {
    var results : ArrayBuffer[Long] = ArrayBuffer();
    val df : DataFrame = executeQuery(connect(), s"select game_id from p1.games order by game_id");
    if (!df.isEmpty)
      if (!df.take(1)(0).isNullAt(0))
        results += df.take(1)(0).getLong(0);

    return results.toList;
  }

  def getMaxGameId() : Long = {
    var result : Long = 0L;
    val df : DataFrame = executeQuery(connect(), s"select max(game_id) from p1.games");
    if (!df.isEmpty)
      if (!df.take(1)(0).isNullAt(0))
        result = df.take(1)(0).getLong(0);

    return result;
  }

  def getGameCount() : Long = {
    var result : Long = 0L;
    val df : DataFrame = executeQuery(connect(), s"select count(*) from p1.games");
    if (!df.isEmpty)
      if (!df.take(1)(0).isNullAt(0))
        result = df.take(1)(0).getLong(0);

    return result;
  }

  def calculateAvgScore(game_id : Long) : Double = {
    var totalScore : Double = Double.NegativeInfinity;
    val reviews : List[(Long, String, String, String, String, String, LocalDateTime, LocalDateTime, Double, String, Long)] = getGameReviews(game_id);
    if (reviews.nonEmpty) {
      for(review : (Long, String, String, String, String, String, LocalDateTime, LocalDateTime, Double, String, Long) <- reviews)
        totalScore += review._9
      return totalScore / reviews.size;
    } else
      return totalScore;
  }

  def getAvgScore(game_id : Long) : Double = {
    var result : Double = Double.NegativeInfinity;
    val game : (Long, String, LocalDateTime, String, String, String, String, Double, Long, Long, List[String], List[String]) = getGame(game_id);
    if (game != null)
      result = game._8;
    return result;
  }

  def updateAvgScore(game_id : Long, newScore : Double) : Double = {
    var result : Double = Double.NegativeInfinity;
    val spark : SparkSession = connect();
    val game : (Long, String, LocalDateTime, String, String, String, String, Double, Long, Long, List[String], List[String]) = getGame(game_id);
    if (game != null) {
      result = game._8;
      if (newScore >= 0.0 && newScore <= 10.0) {
        val spark: SparkSession = connect();
        createGamesCopy("p1.gamesTemp");
        executeDML(spark, s"insert into p1.gamesTemp select * from p1.games where game_id != ${game_id}L");
        executeDML(spark, "drop table p1.games");
        executeDML(spark, "alter table p1.gamesTemp rename to games");
        addGame(
          game._1,
          game._2,
          game._3,
          game._4,
          game._5,
          game._6,
          game._7,
          newScore,
          game._9,
          game._10,
          game._11,
          game._12
        );
      }
    }

    return result;
  }

  def getPreviousGameArticleCount(game_id : Long) : Long = {
    var result : Long = Long.MinValue;
    val game : (Long, String, LocalDateTime, String, String, String, String, Double, Long, Long, List[String], List[String]) = getGame(game_id);
    if (game != null)
      result = game._9;
    return result;
  }

  def updateArticleCount(game_id : Long, newArticleCount : Long) : Long = {
    var result : Long = Long.MinValue;
    val game : (Long, String, LocalDateTime, String, String, String, String, Double, Long, Long, List[String], List[String]) = getGame(game_id);
    if (game != null) {
      result = game._9
      if (newArticleCount >= 0) {
        val spark: SparkSession = connect();
        createGamesCopy("p1.gamesTemp");
        executeDML(spark, s"insert into p1.gamesTemp select * from p1.games where game_id != ${game_id}L");
        executeDML(spark, "drop table p1.games");
        executeDML(spark, "alter table p1.gamesTemp rename to games");
        addGame(
          game._1,
          game._2,
          game._3,
          game._4,
          game._5,
          game._6,
          game._7,
          game._8,
          newArticleCount,
          game._10,
          game._11,
          game._12
        );
      }
    }
    return result;
  }

  def getPreviousGameReviewCount(game_id : Long) : Long = {
    var result : Long = Long.MinValue;
    val game : (Long, String, LocalDateTime, String, String, String, String, Double, Long, Long, List[String], List[String]) = getGame(game_id);
    if (game != null)
      result = game._10
    return result;
  }

  def updateReviewCount(game_id : Long, newReviewCount : Long) : Long = {
    var result : Long = Long.MinValue;
    val game : (Long, String, LocalDateTime, String, String, String, String, Double, Long, Long, List[String], List[String]) = getGame(game_id);
    if (game != null) {
      result = game._10
      if (newReviewCount >= 0) {
        val spark: SparkSession = connect();
        createGamesCopy("p1.gamesTemp");
        executeDML(spark, s"insert into p1.gamesTemp select * from p1.games where game_id != ${game_id}L");
        executeDML(spark, "drop table p1.games");
        executeDML(spark, "alter table p1.gamesTemp rename to games");
        addGame(
          game._1,
          game._2,
          game._3,
          game._4,
          game._5,
          game._6,
          game._7,
          game._8,
          game._9,
          newReviewCount,
          game._11,
          game._12
        );
      }
    }
    return result;
  }

  def deleteGame(game_id : Long) : (Long, String, LocalDateTime, String, String, String, String, Double, Long, Long, List[String], List[String]) = {
    val result : (Long, String, LocalDateTime, String, String, String, String, Double, Long, Long, List[String], List[String]) = getGame(game_id);
    if (result != null) {
      val spark: SparkSession = connect();
      createGamesCopy("p1.gamesTemp");
      executeDML(spark, s"insert into p1.gamesTemp select * from p1.games where game_id != ${game_id}L");
      executeDML(spark, "drop table p1.games");
      executeDML(spark, "alter table p1.gamesTemp rename to games");
    }
    return result;
  }

  def deleteGames(game_ids : List[Long]) : List[(Long, String, LocalDateTime, String, String, String, String, Double, Long, Long, List[String], List[String])] = {
    val result : ArrayBuffer[(Long, String, LocalDateTime, String, String, String, String, Double, Long, Long, List[String], List[String])] = ArrayBuffer();
    for (game_id : Long <- game_ids) {
      val game : (Long, String, LocalDateTime, String, String, String, String, Double, Long, Long, List[String], List[String]) = deleteGame(game_id);
      if (game != null)
        result += game;
    }
    return result.toList;
  }

  def addReview(review_id : Long, authors : String, title : String, deck : String, lede : String, body : String, publish_date : LocalDateTime, update_date : LocalDateTime, score : Double, review_type : String, game_id : Long) : Unit = {
    val publishDate : String = publish_date.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    val updateDate : String = update_date.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    executeDML(connect(),
      s"insert into p1.reviews partition(year='${publish_date.getYear}') select " +
        s"${review_id}L, " +
        s"'$authors', " +
        s"'$title', " +
        s"'$deck', " +
        s"'$lede', " +
        s"'$body', " +
        s"to_timestamp('$publishDate'), " +
        s"to_timestamp('$updateDate'), " +
        s"$score, " +
        s"'$review_type', " +
        s"${game_id}L"
    );
  }

  def addReviews(reviews : List[(Long, String, String, String, String, String, LocalDateTime, LocalDateTime, Double, String, Long)]) : Unit = {
    for(review : (Long, String, String, String, String, String, LocalDateTime, LocalDateTime, Double, String, Long) <- reviews)
      addReview(
        review._1,
        review._2,
        review._3,
        review._4,
        review._5,
        review._6,
        review._7,
        review._8,
        review._9,
        review._10,
        review._11
      );
  }

  def reviewExists(review_id : Long) : Boolean = {
    val df : DataFrame = executeQuery(connect(), s"select * from p1.reviews where review_id = ${review_id}L limit 1");
    if (!df.isEmpty)
      if (!df.take(1)(0).isNullAt(0))
        return true;
    return false;
  }

  def getReview(review_id : Long) : (Long, String, String, String, String, String, LocalDateTime, LocalDateTime, Double, String, Long) = {
    var result : (Long, String, String, String, String, String, LocalDateTime, LocalDateTime, Double, String, Long) = null;
    val df : DataFrame = executeQuery(connect(), s"select * from p1.reviews where review_id = ${review_id}L");
    if (!df.isEmpty)
      if (!df.take(1)(0).isNullAt(0)) {
        val row : Row = df.take(1)(0);
        val tuple : (Long, String, String, String, String, String, LocalDateTime, LocalDateTime, Double, String, Long) = (
          row.getLong(0),
          row.getString(1),
          row.getString(2),
          row.getString(3),
          row.getString(4),
          row.getString(5),
          row.getTimestamp(6).toLocalDateTime,
          row.getTimestamp(7).toLocalDateTime,
          row.getDouble(8),
          row.getString(9),
          row.getLong(10)
        );
        result = tuple;
      }

    return result;
  }

  def getGameReviews(game_id : Long) : List[(Long, String, String, String, String, String, LocalDateTime, LocalDateTime, Double, String, Long)] = {
    var result : ArrayBuffer[(Long, String, String, String, String, String, LocalDateTime, LocalDateTime, Double, String, Long)] = ArrayBuffer();
    val df : DataFrame = executeQuery(connect(), s"select * from p1.reviews where game_id = ${game_id}L");
    if (!df.isEmpty)
      if (!df.take(1)(0).isNullAt(0))
        for (row : Row <- df.collect()) {
          val tuple : (Long, String, String, String, String, String, LocalDateTime, LocalDateTime, Double, String, Long) = (
            row.getLong(0),
            row.getString(1),
            row.getString(2),
            row.getString(3),
            row.getString(4),
            row.getString(5),
            row.getTimestamp(6).toLocalDateTime,
            row.getTimestamp(7).toLocalDateTime,
            row.getDouble(8),
            row.getString(9),
            row.getLong(10)
          );
          result += tuple;
        }

    return result.toList;
  }

  def getReviewCount() : Long = {
    val df : DataFrame = executeQuery(connect(), s"select count(*) from p1.reviews");
    if (!df.isEmpty)
      if (!df.take(1)(0).isNullAt(0))
        return df.take(1)(0).getLong(0);
    return 0L;
  }

  def getGameReviewCount(game_id : Long) : Long = {
    val df : DataFrame = executeQuery(connect(), s"select count(*) from p1.reviews where game_id = $game_id");
    if (!df.isEmpty)
      if (!df.take(1)(0).isNullAt(0))
        return df.take(1)(0).getLong(0);
    return 0L;
  }

    def deleteGameReviews(game_id : Long) : List[(Long, String, String, String, String, String, LocalDateTime, LocalDateTime, Double, String, Long)] = {
    val result : List[(Long, String, String, String, String, String, LocalDateTime, LocalDateTime, Double, String, Long)] = getGameReviews(game_id);

    val spark : SparkSession = connect();
    createReviewsCopy("p1.reviewsTemp");
    executeDML(spark, s"insert into p1.reviewsTemp select * from p1.reviews where game_id != ${game_id}L");
    executeDML(spark, "drop table p1.reviews");
    executeDML(spark, "alter table p1.reviewsTemp rename to reviews");

    return result;
  }

  def addArticle(article_id : Long, authors : String, title : String, deck : String, lede : String, body : String, publish_date : LocalDateTime, update_date : LocalDateTime, categories : Map[Long, String], game_id : Long) : Unit = {
    var categoriesInnerString : String = "";
    for (pair : (Long, String) <- categories)
      categoriesInnerString += s"${pair._1}, '${pair._2}', ";
    val categoriesString : String = "map(" + categoriesInnerString.replaceFirst(", $", "") + ")";

    val publishDate : String = publish_date.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    val updateDate : String = update_date.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

    executeDML(connect(),
      s"insert into p1.articles partition(year='${publish_date.getYear}') select " +
        s"${article_id}L, " +
        s"'$authors', " +
        s"'$title', " +
        s"'$deck', " +
        s"'$lede', " +
        s"'$body', " +
        s"to_timestamp('$publishDate'), " +
        s"to_timestamp('$updateDate'), " +
        s"$categoriesString, " +
        s"${game_id}L"
    );
  }

  def addArticles(articles : List[(Long, String, String, String, String, String, LocalDateTime, LocalDateTime, Map[Long, String], Long)]) : Unit = {
    for (article : (Long, String, String, String, String, String, LocalDateTime, LocalDateTime, Map[Long, String], Long) <- articles)
      addArticle(
        article._1,
        article._2,
        article._3,
        article._4,
        article._5,
        article._6,
        article._7,
        article._8,
        article._9,
        article._10
      );
  }

  def articleExists(article_id : Long) : Boolean = {
    val df : DataFrame = executeQuery(connect(), s"select * from p1.articles where article_id = ${article_id}L limit 1");
    if (!df.isEmpty)
      if (!df.take(1)(0).isNullAt(0))
        return true;
    return false;
  }

  def getArticle(article_id : Long) : (Long, String, String, String, String, String, LocalDateTime, LocalDateTime, Map[Long, String], Long) = {
    var result : (Long, String, String, String, String, String, LocalDateTime, LocalDateTime, Map[Long, String], Long) = null;
    val df : DataFrame = executeQuery(connect(), s"select * from p1.articles where article_id = ${article_id}L");
    if (!df.isEmpty)
      if (!df.take(1)(0).isNullAt(0)) {
        val row : Row = df.take(1)(0);
        val categories : Map[Long, String] = row.get(8).asInstanceOf[Map[Long, String]];
        val tuple : (Long, String, String, String, String, String, LocalDateTime, LocalDateTime, Map[Long, String], Long) = (
          row.getLong(0),
          row.getString(1),
          row.getString(2),
          row.getString(3),
          row.getString(4),
          row.getString(5),
          row.getTimestamp(6).toLocalDateTime,
          row.getTimestamp(7).toLocalDateTime,
          row.get(8).asInstanceOf[Map[Long, String]],
          row.getLong(9)
        );
        result = tuple;
      }

    return result;
  }

  def getGameArticleCount(game_id : Long) : Long = {
    val df : DataFrame = executeQuery(connect(), s"select count(*) from p1.articles where game_id = $game_id");
    if (!df.isEmpty)
      if (!df.take(1)(0).isNullAt(0))
        return df.take(1)(0).getLong(0);
    return 0L;
  }

  def getGameArticles(game_id : Long) : List[(Long, String, String, String, String, String, LocalDateTime, LocalDateTime, Map[Long, String], Long)] = {
    var result : ArrayBuffer[(Long, String, String, String, String, String, LocalDateTime, LocalDateTime, Map[Long, String], Long)] = ArrayBuffer();
    val df : DataFrame = executeQuery(connect(), s"select * from p1.articles where game_id = ${game_id}L");
    if (!df.isEmpty)
      if (!df.take(1)(0).isNullAt(0))
        for (row : Row <- df.collect()) {
          var categories : Map[Long, String] = row.get(8).asInstanceOf[Map[Long, String]];
          val tuple : (Long, String, String, String, String, String, LocalDateTime, LocalDateTime, Map[Long, String], Long) = (
            row.getLong(0),
            row.getString(1),
            row.getString(2),
            row.getString(3),
            row.getString(4),
            row.getString(5),
            row.getTimestamp(6).toLocalDateTime,
            row.getTimestamp(7).toLocalDateTime,
            row.get(8).asInstanceOf[Map[Long, String]],
            row.getLong(9)
          );
          result += tuple;
        }

    return result.toList;
  }

    def deleteGameArticles(game_id : Long) : List[(Long, String, String, String, String, String, LocalDateTime, LocalDateTime, Map[Long, String], Long)] = {
    val result : List[(Long, String, String, String, String, String, LocalDateTime, LocalDateTime, Map[Long, String], Long)] = getGameArticles(game_id);
    val spark : SparkSession = connect();
    createArticlesCopy("p1.articlesTemp");
    executeDML(spark, s"insert into p1.articlesTemp select * from p1.articles where game_id != ${game_id}L");
    executeDML(spark, "drop table p1.articles");
    executeDML(spark, "alter table p1.articlesTemp rename to articles");

    return result;
  }
}

object HiveDBManager extends HiveConnection {
  protected def createDB() : Unit = {
    val spark : SparkSession = connect();
    executeDML(spark, "create database if not exists p1");
    createUsersCopy("p1.users");
    createQueriesCopy("p1.queries");
    createGamesCopy("p1.games");
    createReviewsCopy("p1.reviews");
    createArticlesCopy("p1.articles");

    executeDML(spark, s"insert into p1.users values (${getNextUserId()}, 'admin', '${"admin".sha512.hex}', true)");
  }

  private def createUsersCopy(table_name : String) : Unit = {
    executeDML(connect(),
      "create table if not exists " +
        s"$table_name(" +
        "user_id Int, " +
        "username String, " +
        "password String, " +
        "isAdmin Boolean " +
        ") " +
        "stored as orc"
    );
  }

  private def createQueriesCopy(table_name : String) : Unit = {
    executeDML(connect(),
      "create table if not exists " +
        s"$table_name(" +
        "query_id Int, " +
        "user_id Int, " +
        "query_name String, " +
        "query String " +
        ") " +
        "stored as orc"
    );
  }

  private def createGamesCopy(table_name : String) : Unit = {
    executeDML(connect(),
      "create table if not exists " +
        s"$table_name(" +
        "game_id BigInt, " +
        "name String, " +
        "release_date Timestamp, " +
        "deck String, " +
        "description String, " +
        "articles_api_url String, " +
        "reviews_api_url String, " +
        "avg_score Double, " +
        "article_count BigInt, " +
        "review_count BigInt, " +
        "genres Array<String>, " +
        "themes Array<String> " +
        ") " +
        "partitioned by (year String) " +
        "clustered by (genres, themes) " +
        "sorted by (release_date) " +
        "into 50 buckets " +
        "stored as orc"
    );
  }

  private def createArticlesCopy(table_name : String) : Unit = {
    executeDML(connect(),
      "create table if not exists " +
        s"$table_name(" +
        "article_id BigInt, " +
        "authors String, " +
        "title String, " +
        "deck String, " +
        "lede String, " +
        "body String, " +
        "publish_date Timestamp, " +
        "update_date Timestamp, " +
        "categories map<BigInt, String>, " +
        "game_id BigInt" +
        ") " +
        "partitioned by (year String) " +
        "clustered by (game_id) " +
        "sorted by (publish_date) " +
        "into 10000 buckets " +
        "stored as orc"
    );
  }

  private def createReviewsCopy(table_name : String) : Unit = {
    executeDML(connect(),
      "create table if not exists " +
        s"$table_name(" +
        "review_id BigInt, " +
        "authors String, " +
        "title String, " +
        "deck String, " +
        "lede String, " +
        "body String, " +
        "publish_date Timestamp, " +
        "update_date Timestamp, " +
        "score Double, " +
        "review_type String, " +
        "game_id BigInt" +
        ") " +
        "partitioned by (year String) " +
        "clustered by (review_type) " +
        "sorted by (publish_date) " +
        "into 3 buckets " +
        "stored as orc"
    );
  }

  def getNextUserId() : Int = {
    var result : Int = 1;
    val spark : SparkSession = connect();
    val df : DataFrame = executeQuery(spark, "select max(user_id) from p1.users");
    if (!df.isEmpty)
      if (!df.take(1)(0).isNullAt(0))
        result = df.take(1)(0).getInt(0) + 1

    return result;
  }

  protected def getUsers() : List[(Int, String, String, Boolean)] = {
    var result : ArrayBuffer[(Int, String, String, Boolean)] = ArrayBuffer();
    val spark : SparkSession = connect();
    val df : DataFrame = executeQuery(spark, "select user_id, username, password, isAdmin from p1.users order by user_id");
    if (!df.isEmpty)
      if (!df.take(1)(0).isNullAt(0))
        for (r : Row <- df.collect()) {
          val tuple : (Int, String, String, Boolean) = (r.getInt(0), r.getString(1), r.getString(2), r.getBoolean(3));
          result += tuple;
        };

    return result.toList;
  }

  def authenticate(username : String, password : String, isAdmin : Boolean) :  Int = {
    var result : Int = 0;
    val spark : SparkSession = connect();
    val df : DataFrame = executeQuery(spark, s"select user_id from p1.users where username = '$username' and password = '${password.sha512.hex}' and isAdmin = $isAdmin");
    if (!df.isEmpty)
      if (!df.take(1)(0).isNullAt(0))
        result = df.take(1)(0).getInt(0);

    return result;
  }

  def addUser(username : String, password : String) : Int = {
    val result : Int = getNextUserId();
    executeDML(connect(), s"insert into p1.users values ($result, '$username', '${password.sha512.hex}', false)");
    return result;
  }

  def updateUsername(user_id : Int, newUsername : String) : Boolean = {
    val spark : SparkSession = connect();
    val df : DataFrame = executeQuery(spark, s"select password, isAdmin from p1.users where user_id = $user_id");
    val passAdmin : (String, Boolean) = (df.take(1)(0).getString(0), df.take(1)(0).getBoolean(1));
    createUsersCopy("p1.usersTemp");
    executeDML(spark, s"insert into p1.usersTemp select * from p1.users where user_id != $user_id");
    executeDML(spark, "drop table p1.users");
    executeDML(spark, "alter table p1.usersTemp rename to users");
    executeDML(spark, s"insert into table p1.users values ($user_id, '$newUsername', '${passAdmin._1}', ${passAdmin._2})");
    return true;
  }

  def updatePassword(user_id : Int, oldPassword : String, newPassword : String) : Boolean = {
    val spark : SparkSession = connect();
    var df : DataFrame = executeQuery(spark, s"select * from p1.users where user_id = $user_id and password = '${oldPassword.sha512.hex}'");
    if (df.isEmpty)
      return false;
    else {
      df = executeQuery(spark, s"select username, isAdmin from p1.users where user_id = $user_id");
      val userAdmin : (String, Boolean) = (df.take(1)(0).getString(0), df.take(1)(0).getBoolean(1));
      createUsersCopy("p1.usersTemp");
      executeDML(spark, s"insert into p1.usersTemp select * from p1.users where user_id != $user_id");
      executeDML(spark, "drop table p1.users");
      executeDML(spark, "alter table p1.usersTemp rename to users");
      executeDML(spark, s"insert into table p1.users values ($user_id, '${userAdmin._1}', '${newPassword.sha512.hex}', ${userAdmin._2})");
      return true;
    }
  }

  def usernameExists(username : String) : Boolean = {
    val spark : SparkSession = connect();
    val df : DataFrame = executeQuery(spark, s"select username from p1.users where username = '$username'");
    return !df.isEmpty;
  }

  protected def isAdmin(user_id : Int) : Boolean = {
    val spark : SparkSession = connect();
    val df : DataFrame = executeQuery(spark, s"select isAdmin from p1.users where user_id = $user_id");
    return df.take(1)(0).getBoolean(0);
  }

  protected def setToAdmin(user_id : Int) : Unit = {
    val spark : SparkSession = connect();
    var df : DataFrame = executeQuery(spark, s"select username, password from p1.users where user_id = $user_id");
    val userPass : (String, String) = (df.take(1)(0).getString(0), df.take(1)(0).getString(1));
    createUsersCopy("p1.usersTemp");
    executeDML(spark, s"insert into p1.usersTemp select * from p1.users where user_id != $user_id");
    executeDML(spark, "drop table p1.users");
    executeDML(spark, "alter table p1.usersTemp rename to users");
    executeDML(spark, s"insert into table p1.users values ($user_id, '${userPass._1}', '${userPass._2}', true)");
  }

  protected def deleteUser(user_id : Int) : Unit = {
    val spark : SparkSession = connect();
    createUsersCopy("p1.usersTemp");
    executeDML(spark, s"insert into p1.usersTemp select * from p1.users where user_id != $user_id");
    executeDML(spark, "drop table p1.users");
    executeDML(spark, "alter table p1.usersTemp rename to users");
  }

  protected def getNextQueryId() : Int = {
    var result : Int = 1;
    val spark : SparkSession = connect();
    val df : DataFrame = executeQuery(spark, "select max(query_id) from p1.queries");
    if (!df.isEmpty)
      if (!df.take(1)(0).isNullAt(0))
        result = df.take(1)(0).getInt(0);

    return result;
  }

  def getQueries() : Map[Int, String] = {
    var result : Map[Int, String] = Map();
    val spark : SparkSession = connect();
    val df : DataFrame = executeQuery(spark, "select query_id, query_name from p1.queries order by query_id");
    if (!df.isEmpty)
      if (!df.take(1)(0).isNullAt(0))
        for (row : Row <- df.collect())
          result += (row.getInt(0) -> row.getString(1));
    return result;
  }

  def queryNameExists(query_name : String) : Boolean = {
    val queries : Map[Int, String] = getQueries();
    if (queries.isEmpty)
      return false;
    else
      return queries.values.find(q => q.equals(query_name)).get.nonEmpty;
  }

  def showQuery(query_id : Int) : Unit = {
    val spark : SparkSession = connect();
    val df : DataFrame = executeQuery(spark, s"select query from p1.queries where query_id = $query_id");
    if (!df.isEmpty)
      if (!df.take(1)(0).isNullAt(0))
        showQuery(spark, df.take(1)(0).getString(0));
  }

  protected def showQuery(query : String) : Unit = {
    showQuery(connect(), query);
  }

  protected def saveQuery(user_id : Int, query_name : String, query : String) : Unit = {
    executeDML(connect(), s"insert into p1.queries values (${getNextQueryId()}, $user_id, '$query_name', '$query')");
  }

  protected def deleteQuery(query_id : Int) : Unit = {
    val spark : SparkSession = connect();
    createQueriesCopy("p1.queriesTemp");
    executeDML(spark, s"insert into p1.queriesTemp select * from p1.queries where query_id != $query_id");
    executeDML(spark, "drop table p1.queries");
    executeDML(spark, "alter table p1.queriesTemp rename to queries");
  }

  def addGame(game_id : Long, name : String, release_date : LocalDateTime, deck : String, description : String, articles_api_url : String, reviews_api_url : String, avg_score : Double, article_count : Long, review_count : Long, genres : List[String], themes : List[String]) : Unit = {
    val genresString : String = "'" + genres.mkString("','") + "'";
    val themesString : String = "'" + themes.mkString("','") + "'";
    val dateTime : String = release_date.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    executeDML(connect(),
      s"insert into p1.games partition(year='${release_date.getYear}') select " +
        s"${game_id}L, " +
        s"'$name', " +
        s"to_timestamp('$dateTime'), " +
        s"'$deck', " +
        s"'$description', " +
        s"'$articles_api_url', " +
        s"'$reviews_api_url', " +
        s"$avg_score, " +
        s"${article_count}L, " +
        s"${review_count}L, " +
        s"ARRAY($genresString), " +
        s"ARRAY($themesString)"
    );
  }

  def addGames(games : List[(Long, String, LocalDateTime, String, String, String, String, Double, Long, Long, List[String], List[String])]) : Unit = {
    for (game : (Long, String, LocalDateTime, String, String, String, String, Double, Long, Long, List[String], List[String]) <- games)
      addGame(
        game._1,
        game._2,
        game._3,
        game._4,
        game._5,
        game._6,
        game._7,
        game._8,
        game._9,
        game._10,
        game._11,
        game._12
      );
  }

  def gameExists(game_id : Long, name : String) : Boolean = {
    val game : (Long, String, LocalDateTime, String, String, String, String, Double, Long, Long, List[String], List[String]) = getGame(game_id);
    if (game == null)
      return false;
    else
      return game._2.equals(name);
  }

  def getGame(game_id : Long) : (Long, String, LocalDateTime, String, String, String, String, Double, Long, Long, List[String], List[String]) = {
    var result : (Long, String, LocalDateTime, String, String, String, String, Double, Long, Long, List[String], List[String]) = null;
    val df : DataFrame = executeQuery(connect(), s"select * from p1.games where game_id = ${game_id}L limit 1");
    if (!df.isEmpty) {
      val row : Row = df.take(1)(0);
      val genres : List[String] = row.get(10).asInstanceOf[mutable.WrappedArray[String]].toList;
      val themes : List[String] = row.get(11).asInstanceOf[mutable.WrappedArray[String]].toList;
      result = (
        row.getLong(0),
        row.getString(1),
        row.getTimestamp(2).toLocalDateTime,
        row.getString(3),
        row.getString(4),
        row.getString(5),
        row.getString(6),
        row.getDouble(7),
        row.getLong(8),
        row.getLong(9),
        genres,
        themes
      );
    }
    return result;
  }

  def getGame(game_name : String) : (Long, String, LocalDateTime, String, String, String, String, Double, Long, Long, List[String], List[String]) = {
    var result : (Long, String, LocalDateTime, String, String, String, String, Double, Long, Long, List[String], List[String]) = null;
    val df : DataFrame = executeQuery(connect(), s"select * from p1.games where name = '$game_name' limit 1");
    if (!df.isEmpty) {
      val row : Row = df.take(1)(0);
      val genres : List[String] = row.get(10).asInstanceOf[mutable.WrappedArray[String]].toList;
      val themes : List[String] = row.get(11).asInstanceOf[mutable.WrappedArray[String]].toList;
      result = (
        row.getLong(0),
        row.getString(1),
        row.getTimestamp(2).toLocalDateTime,
        row.getString(3),
        row.getString(4),
        row.getString(5),
        row.getString(6),
        row.getDouble(7),
        row.getLong(8),
        row.getLong(9),
        genres,
        themes
      );
    }
    return result;
  }

  def getGamesBetween(startDate : LocalDateTime, endDate : LocalDateTime) : List[(Long, String, LocalDateTime, String, String, String, String, Double, Long, Long, List[String], List[String])] = {
    var result : ArrayBuffer[(Long, String, LocalDateTime, String, String, String, String, Double, Long, Long, List[String], List[String])] = ArrayBuffer();
    val df : DataFrame = executeQuery(connect(), s"select * from p1.games where release_date between '${Timestamp.valueOf(startDate)}' and '${Timestamp.valueOf(endDate)}' order by game_id")
    if (!df.isEmpty)
      if (!df.take(1)(0).isNullAt(0))
        for(row : Row <- df.collect()) {
          val genres : List[String] = row.get(10).asInstanceOf[mutable.WrappedArray[String]].toList;
          val themes : List[String] = row.get(11).asInstanceOf[mutable.WrappedArray[String]].toList;
          val tuple : (Long, String, LocalDateTime, String, String, String, String, Double, Long, Long, List[String], List[String]) = (
            row.getLong(0),
            row.getString(1),
            row.getTimestamp(2).toLocalDateTime,
            row.getString(3),
            row.getString(4),
            row.getString(5),
            row.getString(6),
            row.getDouble(7),
            row.getLong(8),
            row.getLong(9),
            genres,
            themes
          );
          result += tuple;
        }

    return result.toList;
  }

  def getMaxGameIdBetween(startDate : LocalDateTime, endDate : LocalDateTime) : Long = {
    var result : Long = 0L;
    val df : DataFrame = executeQuery(connect(), s"select max(game_id) from p1.games where release_date between '${Timestamp.valueOf(startDate)}' and '${Timestamp.valueOf(endDate)}'");
    if (!df.isEmpty)
      if (!df.take(1)(0).isNullAt(0))
        result = df.take(1)(0).getLong(0);

    return result;
  }

  def getGameCountBetween(startDate : LocalDateTime, endDate : LocalDateTime) : Long = {
    var result : Long = 0L;
    val df : DataFrame = executeQuery(connect(), s"select count(*) from p1.games where release_date between '${Timestamp.valueOf(startDate)}' and '${Timestamp.valueOf(endDate)}'");
    if (!df.isEmpty)
      if (!df.take(1)(0).isNullAt(0))
        result = df.take(1)(0).getLong(0);

    return result;
  }

  def getLatestGames() : List[(Long, String, LocalDateTime, String, String, String, String, Double, Long, Long, List[String], List[String])] = {
    var results : ArrayBuffer[(Long, String, LocalDateTime, String, String, String, String, Double, Long, Long, List[String], List[String])] = ArrayBuffer();
    val df : DataFrame = executeQuery(connect(), "select * from p1.games where release_date = (select max(release_date) from p1.games)");
    if (!df.isEmpty)
      if (!df.take(1)(0).isNullAt(0)) {
        for(row : Row <- df.collect()) {
          val genres : List[String] = row.get(10).asInstanceOf[mutable.WrappedArray[String]].toList;
          val themes : List[String] = row.get(11).asInstanceOf[mutable.WrappedArray[String]].toList;
          val game : (Long, String, LocalDateTime, String, String, String, String, Double, Long, Long, List[String], List[String]) = (
            row.getLong(0),
            row.getString(1),
            row.getTimestamp(2).toLocalDateTime,
            row.getString(3),
            row.getString(4),
            row.getString(5),
            row.getString(6),
            row.getDouble(7),
            row.getLong(8),
            row.getLong(9),
            genres,
            themes
          );
          results += game;
        }
      };
    return results.toList;
  }

  def getGameIds() : List[Long] = {
    var results : ArrayBuffer[Long] = ArrayBuffer();
    val df : DataFrame = executeQuery(connect(), s"select game_id from p1.games order by game_id");
    if (!df.isEmpty)
      if (!df.take(1)(0).isNullAt(0))
        results += df.take(1)(0).getLong(0);

    return results.toList;
  }

  def getMaxGameId() : Long = {
    var result : Long = 0L;
    val df : DataFrame = executeQuery(connect(), s"select max(game_id) from p1.games");
    if (!df.isEmpty)
      if (!df.take(1)(0).isNullAt(0))
        result = df.take(1)(0).getLong(0);

    return result;
  }

  def getGameCount() : Long = {
    var result : Long = 0L;
    val df : DataFrame = executeQuery(connect(), s"select count(*) from p1.games");
    if (!df.isEmpty)
      if (!df.take(1)(0).isNullAt(0))
        result = df.take(1)(0).getLong(0);

    return result;
  }

  def calculateAvgScore(game_id : Long) : Double = {
    var totalScore : Double = Double.NegativeInfinity;
    val reviews : List[(Long, String, String, String, String, String, LocalDateTime, LocalDateTime, Double, String, Long)] = getGameReviews(game_id);
    if (reviews.nonEmpty) {
      for(review : (Long, String, String, String, String, String, LocalDateTime, LocalDateTime, Double, String, Long) <- reviews)
        totalScore += review._9
      return totalScore / reviews.size;
    } else
      return totalScore;
  }

  def getAvgScore(game_id : Long) : Double = {
    var result : Double = Double.NegativeInfinity;
    val game : (Long, String, LocalDateTime, String, String, String, String, Double, Long, Long, List[String], List[String]) = getGame(game_id);
    if (game != null)
      result = game._8;
    return result;
  }

  def updateAvgScore(game_id : Long, newScore : Double) : Double = {
    var result : Double = Double.NegativeInfinity;
    val spark : SparkSession = connect();
    val game : (Long, String, LocalDateTime, String, String, String, String, Double, Long, Long, List[String], List[String]) = getGame(game_id);
    if (game != null) {
      result = game._8;
      if (newScore >= 0.0 && newScore <= 10.0) {
        val spark: SparkSession = connect();
        createGamesCopy("p1.gamesTemp");
        executeDML(spark, s"insert into p1.gamesTemp select * from p1.games where game_id != ${game_id}L");
        executeDML(spark, "drop table p1.games");
        executeDML(spark, "alter table p1.gamesTemp rename to games");
        addGame(
          game._1,
          game._2,
          game._3,
          game._4,
          game._5,
          game._6,
          game._7,
          newScore,
          game._9,
          game._10,
          game._11,
          game._12
        );
      }
    }

    return result;
  }

  def getPreviousGameArticleCount(game_id : Long) : Long = {
    var result : Long = Long.MinValue;
    val game : (Long, String, LocalDateTime, String, String, String, String, Double, Long, Long, List[String], List[String]) = getGame(game_id);
    if (game != null)
      result = game._9;
    return result;
  }

  def updateArticleCount(game_id : Long, newArticleCount : Long) : Long = {
    var result : Long = Long.MinValue;
    val game : (Long, String, LocalDateTime, String, String, String, String, Double, Long, Long, List[String], List[String]) = getGame(game_id);
    if (game != null) {
      result = game._9
      if (newArticleCount >= 0) {
        val spark: SparkSession = connect();
        createGamesCopy("p1.gamesTemp");
        executeDML(spark, s"insert into p1.gamesTemp select * from p1.games where game_id != ${game_id}L");
        executeDML(spark, "drop table p1.games");
        executeDML(spark, "alter table p1.gamesTemp rename to games");
        addGame(
          game._1,
          game._2,
          game._3,
          game._4,
          game._5,
          game._6,
          game._7,
          game._8,
          newArticleCount,
          game._10,
          game._11,
          game._12
        );
      }
    }
    return result;
  }

  def getPreviousGameReviewCount(game_id : Long) : Long = {
    var result : Long = Long.MinValue;
    val game : (Long, String, LocalDateTime, String, String, String, String, Double, Long, Long, List[String], List[String]) = getGame(game_id);
    if (game != null)
      result = game._10
    return result;
  }

  def updateReviewCount(game_id : Long, newReviewCount : Long) : Long = {
    var result : Long = Long.MinValue;
    val game : (Long, String, LocalDateTime, String, String, String, String, Double, Long, Long, List[String], List[String]) = getGame(game_id);
    if (game != null) {
      result = game._10
      if (newReviewCount >= 0) {
        val spark: SparkSession = connect();
        createGamesCopy("p1.gamesTemp");
        executeDML(spark, s"insert into p1.gamesTemp select * from p1.games where game_id != ${game_id}L");
        executeDML(spark, "drop table p1.games");
        executeDML(spark, "alter table p1.gamesTemp rename to games");
        addGame(
          game._1,
          game._2,
          game._3,
          game._4,
          game._5,
          game._6,
          game._7,
          game._8,
          game._9,
          newReviewCount,
          game._11,
          game._12
        );
      }
    }
    return result;
  }

  def deleteGame(game_id : Long) : (Long, String, LocalDateTime, String, String, String, String, Double, Long, Long, List[String], List[String]) = {
    val result : (Long, String, LocalDateTime, String, String, String, String, Double, Long, Long, List[String], List[String]) = getGame(game_id);
    if (result != null) {
      val spark: SparkSession = connect();
      createGamesCopy("p1.gamesTemp");
      executeDML(spark, s"insert into p1.gamesTemp select * from p1.games where game_id != ${game_id}L");
      executeDML(spark, "drop table p1.games");
      executeDML(spark, "alter table p1.gamesTemp rename to games");
    }
    return result;
  }

  def deleteGames(game_ids : List[Long]) : List[(Long, String, LocalDateTime, String, String, String, String, Double, Long, Long, List[String], List[String])] = {
    val result : ArrayBuffer[(Long, String, LocalDateTime, String, String, String, String, Double, Long, Long, List[String], List[String])] = ArrayBuffer();
    for (game_id : Long <- game_ids) {
      val game : (Long, String, LocalDateTime, String, String, String, String, Double, Long, Long, List[String], List[String]) = deleteGame(game_id);
      if (game != null)
        result += game;
    }
    return result.toList;
  }

  def addReview(review_id : Long, authors : String, title : String, deck : String, lede : String, body : String, publish_date : LocalDateTime, update_date : LocalDateTime, score : Double, review_type : String, game_id : Long) : Unit = {
    val publishDate : String = publish_date.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    val updateDate : String = update_date.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    executeDML(connect(),
      s"insert into p1.reviews partition(year='${publish_date.getYear}') select " +
        s"${review_id}L, " +
        s"'$authors', " +
        s"'$title', " +
        s"'$deck', " +
        s"'$lede', " +
        s"'$body', " +
        s"to_timestamp('$publishDate'), " +
        s"to_timestamp('$updateDate'), " +
        s"$score, " +
        s"'$review_type', " +
        s"${game_id}L"
    );
  }

  def addReviews(reviews : List[(Long, String, String, String, String, String, LocalDateTime, LocalDateTime, Double, String, Long)]) : Unit = {
    for(review : (Long, String, String, String, String, String, LocalDateTime, LocalDateTime, Double, String, Long) <- reviews)
      addReview(
        review._1,
        review._2,
        review._3,
        review._4,
        review._5,
        review._6,
        review._7,
        review._8,
        review._9,
        review._10,
        review._11
      );
  }

  def reviewExists(review_id : Long) : Boolean = {
    val df : DataFrame = executeQuery(connect(), s"select * from p1.reviews where review_id = ${review_id}L limit 1");
    if (!df.isEmpty)
      if (!df.take(1)(0).isNullAt(0))
        return true;
    return false;
  }

  def getReview(review_id : Long) : (Long, String, String, String, String, String, LocalDateTime, LocalDateTime, Double, String, Long) = {
    var result : (Long, String, String, String, String, String, LocalDateTime, LocalDateTime, Double, String, Long) = null;
    val df : DataFrame = executeQuery(connect(), s"select * from p1.reviews where review_id = ${review_id}L");
    if (!df.isEmpty)
      if (!df.take(1)(0).isNullAt(0)) {
        val row : Row = df.take(1)(0);
        val tuple : (Long, String, String, String, String, String, LocalDateTime, LocalDateTime, Double, String, Long) = (
          row.getLong(0),
          row.getString(1),
          row.getString(2),
          row.getString(3),
          row.getString(4),
          row.getString(5),
          row.getTimestamp(6).toLocalDateTime,
          row.getTimestamp(7).toLocalDateTime,
          row.getDouble(8),
          row.getString(9),
          row.getLong(10)
        );
        result = tuple;
      }

    return result;
  }

  def getGameReviews(game_id : Long) : List[(Long, String, String, String, String, String, LocalDateTime, LocalDateTime, Double, String, Long)] = {
    var result : ArrayBuffer[(Long, String, String, String, String, String, LocalDateTime, LocalDateTime, Double, String, Long)] = ArrayBuffer();
    val df : DataFrame = executeQuery(connect(), s"select * from p1.reviews where game_id = ${game_id}L");
    if (!df.isEmpty)
      if (!df.take(1)(0).isNullAt(0))
        for (row : Row <- df.collect()) {
          val tuple : (Long, String, String, String, String, String, LocalDateTime, LocalDateTime, Double, String, Long) = (
            row.getLong(0),
            row.getString(1),
            row.getString(2),
            row.getString(3),
            row.getString(4),
            row.getString(5),
            row.getTimestamp(6).toLocalDateTime,
            row.getTimestamp(7).toLocalDateTime,
            row.getDouble(8),
            row.getString(9),
            row.getLong(10)
          );
          result += tuple;
        }

    return result.toList;
  }

  def getReviewCount() : Long = {
    val df : DataFrame = executeQuery(connect(), s"select count(*) from p1.reviews");
    if (!df.isEmpty)
      if (!df.take(1)(0).isNullAt(0))
        return df.take(1)(0).getLong(0);
    return 0L;
  }

  def getGameReviewCount(game_id : Long) : Long = {
    val df : DataFrame = executeQuery(connect(), s"select count(*) from p1.reviews where game_id = $game_id");
    if (!df.isEmpty)
      if (!df.take(1)(0).isNullAt(0))
        return df.take(1)(0).getLong(0);
    return 0L;
  }

  def deleteGameReviews(game_id : Long) : List[(Long, String, String, String, String, String, LocalDateTime, LocalDateTime, Double, String, Long)] = {
    val result : List[(Long, String, String, String, String, String, LocalDateTime, LocalDateTime, Double, String, Long)] = getGameReviews(game_id);

    val spark : SparkSession = connect();
    createReviewsCopy("p1.reviewsTemp");
    executeDML(spark, s"insert into p1.reviewsTemp select * from p1.reviews where game_id != ${game_id}L");
    executeDML(spark, "drop table p1.reviews");
    executeDML(spark, "alter table p1.reviewsTemp rename to reviews");

    return result;
  }

  def addArticle(article_id : Long, authors : String, title : String, deck : String, lede : String, body : String, publish_date : LocalDateTime, update_date : LocalDateTime, categories : Map[Long, String], game_id : Long) : Unit = {
    var categoriesInnerString : String = "";
    for (pair : (Long, String) <- categories)
      categoriesInnerString += s"${pair._1}, '${pair._2}', ";
    val categoriesString : String = "map(" + categoriesInnerString.replaceFirst(", $", "") + ")";

    val publishDate : String = publish_date.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    val updateDate : String = update_date.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

    executeDML(connect(),
      s"insert into p1.articles partition(year='${publish_date.getYear}') select " +
        s"${article_id}L, " +
        s"'$authors', " +
        s"'$title', " +
        s"'$deck', " +
        s"'$lede', " +
        s"'$body', " +
        s"to_timestamp('$publishDate'), " +
        s"to_timestamp('$updateDate'), " +
        s"$categoriesString, " +
        s"${game_id}L"
    );
  }

  def addArticles(articles : List[(Long, String, String, String, String, String, LocalDateTime, LocalDateTime, Map[Long, String], Long)]) : Unit = {
    for (article : (Long, String, String, String, String, String, LocalDateTime, LocalDateTime, Map[Long, String], Long) <- articles)
      addArticle(
        article._1,
        article._2,
        article._3,
        article._4,
        article._5,
        article._6,
        article._7,
        article._8,
        article._9,
        article._10
      );
  }

  def articleExists(article_id : Long) : Boolean = {
    val df : DataFrame = executeQuery(connect(), s"select * from p1.articles where article_id = ${article_id}L limit 1");
    if (!df.isEmpty)
      if (!df.take(1)(0).isNullAt(0))
        return true;
    return false;
  }

  def getArticle(article_id : Long) : (Long, String, String, String, String, String, LocalDateTime, LocalDateTime, Map[Long, String], Long) = {
    var result : (Long, String, String, String, String, String, LocalDateTime, LocalDateTime, Map[Long, String], Long) = null;
    val df : DataFrame = executeQuery(connect(), s"select * from p1.articles where article_id = ${article_id}L");
    if (!df.isEmpty)
      if (!df.take(1)(0).isNullAt(0)) {
        val row : Row = df.take(1)(0);
        val categories : Map[Long, String] = row.get(8).asInstanceOf[Map[Long, String]];
        val tuple : (Long, String, String, String, String, String, LocalDateTime, LocalDateTime, Map[Long, String], Long) = (
          row.getLong(0),
          row.getString(1),
          row.getString(2),
          row.getString(3),
          row.getString(4),
          row.getString(5),
          row.getTimestamp(6).toLocalDateTime,
          row.getTimestamp(7).toLocalDateTime,
          row.get(8).asInstanceOf[Map[Long, String]],
          row.getLong(9)
        );
        result = tuple;
      }

    return result;
  }

  def getGameArticleCount(game_id : Long) : Long = {
    val df : DataFrame = executeQuery(connect(), s"select count(*) from p1.articles where game_id = $game_id");
    if (!df.isEmpty)
      if (!df.take(1)(0).isNullAt(0))
        return df.take(1)(0).getLong(0);
    return 0L;
  }

  def getGameArticles(game_id : Long) : List[(Long, String, String, String, String, String, LocalDateTime, LocalDateTime, Map[Long, String], Long)] = {
    var result : ArrayBuffer[(Long, String, String, String, String, String, LocalDateTime, LocalDateTime, Map[Long, String], Long)] = ArrayBuffer();
    val df : DataFrame = executeQuery(connect(), s"select * from p1.articles where game_id = ${game_id}L");
    if (!df.isEmpty)
      if (!df.take(1)(0).isNullAt(0))
        for (row : Row <- df.collect()) {
          var categories : Map[Long, String] = row.get(8).asInstanceOf[Map[Long, String]];
          val tuple : (Long, String, String, String, String, String, LocalDateTime, LocalDateTime, Map[Long, String], Long) = (
            row.getLong(0),
            row.getString(1),
            row.getString(2),
            row.getString(3),
            row.getString(4),
            row.getString(5),
            row.getTimestamp(6).toLocalDateTime,
            row.getTimestamp(7).toLocalDateTime,
            row.get(8).asInstanceOf[Map[Long, String]],
            row.getLong(9)
          );
          result += tuple;
        }

    return result.toList;
  }

  def deleteGameArticles(game_id : Long) : List[(Long, String, String, String, String, String, LocalDateTime, LocalDateTime, Map[Long, String], Long)] = {
    val result : List[(Long, String, String, String, String, String, LocalDateTime, LocalDateTime, Map[Long, String], Long)] = getGameArticles(game_id);
    val spark : SparkSession = connect();
    createArticlesCopy("p1.articlesTemp");
    executeDML(spark, s"insert into p1.articlesTemp select * from p1.articles where game_id != ${game_id}L");
    executeDML(spark, "drop table p1.articles");
    executeDML(spark, "alter table p1.articlesTemp rename to articles");

    return result;
  }
}