import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import com.roundeights.hasher.Implicits._

import java.io.{BufferedReader, File, FileReader, FileWriter}
import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.collection.mutable.ArrayBuffer


class HiveDBManager extends HiveConnection {
  protected def createDB() : Unit = {
    val spark : SparkSession = connect();
    executeDML(spark, "create database if not exists p1");
    createUsersCopy("p1.users");
    createQueriesCopy("p1.queries");
    createGamesCopy("p1.games");
    createGameGenresCopy("p1.gameGenres");
    createGenresCopy("p1.genres");
    createGameThemesCopy("p1.gameThemes");
    createThemesCopy("p1.themes");
    createReviewsCopy("p1.reviews");
    createReviewAuthorsCopy("p1.reviewAuthors");
    createArticlesCopy("p1.articles");
    createArticleAuthorsCopy("p1.articleAuthors");
    createAuthorsCopy("p1.authors");
    createArticleCategoriesCopy("p1.articleCategories");
    createCategoriesCopy("p1.categories");

    executeDML(spark, s"insert into p1.users values (${getNextUserId()}, 'admin', '${"admin".sha512.hex}', true)");
  }

  def startupDB() : Unit = {
    val spark : SparkSession = connect();
  }

  protected def createUsersCopy(table_name : String) : Unit = {
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

  protected def createQueriesCopy(table_name : String) : Unit = {
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

  protected def createGamesCopy(table_name : String) : Unit = {
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
          "article_count BigInt, " +
          "review_count BigInt" +
        ") " +
        "partitioned by (year String) " +
        "clustered by (year) " +
        "sorted by (release_date) " +
        "into 50 buckets " +
        "stored as orc"
    );
  }

  protected def createGenresCopy(table_name : String) : Unit = {
    executeDML(connect(),
      "create table if not exists " +
        s"$table_name(" +
        "genre_id BigInt, " +
        "genre String" +
        ") " +
        "stored as orc"
    );
  }

  protected def createGameGenresCopy(table_name : String) : Unit = {
    executeDML(connect(),
      "create table if not exists " +
        s"$table_name(" +
        "genre_id BigInt " +
      ") " +
      "partitioned by (game_id BigInt) " +
      "stored as orc"
    );
  }

  protected def createThemesCopy(table_name : String) : Unit = {
    executeDML(connect(),
      "create table if not exists " +
        s"$table_name(" +
        "theme_id BigInt, " +
        "theme String" +
        ") " +
        "stored as orc"
    );
  }

  protected def createGameThemesCopy(table_name : String) : Unit = {
    executeDML(connect(),
      "create table if not exists " +
        s"$table_name(" +
        "theme_id BigInt " +
        ") " +
        "partitioned by (game_id BigInt) " +
        "stored as orc"
    );
  }

  protected def createArticlesCopy(table_name : String) : Unit = {
    executeDML(connect(),
      "create table if not exists " +
        s"$table_name(" +
          "article_id BigInt, " +
          "title String, " +
          "deck String, " +
          "lede String, " +
          "body String, " +
          "publish_date Timestamp, " +
          "update_date Timestamp " +
        ") " +
        "partitioned by (game_id BigInt, year String) " +
        "stored as orc"
    );
  }

  protected def createCategoriesCopy(table_name : String) : Unit = {
    executeDML(connect(),
      "create table if not exists " +
        s"$table_name(" +
        "category_id BigInt, " +
        "category String" +
      ") " +
      "stored as orc"
    );
  }

  protected def createArticleCategoriesCopy(table_name : String) : Unit = {
    executeDML(connect(),
      "create table if not exists " +
        s"$table_name(" +
        "category_id BigInt " +
        ") " +
        "partitioned by (article_id BigInt) " +
        "stored as orc"
    );
  }

  protected def createArticleAuthorsCopy(table_name : String) : Unit = {
    executeDML(connect(),
      "create table if not exists " +
        s"$table_name(" +
        "author_id BigInt " +
        ") " +
        "partitioned by (article_id BigInt) " +
        "stored as orc"
    );
  }

  protected def createReviewsCopy(table_name : String) : Unit = {
    executeDML(connect(),
      "create table if not exists " +
        s"$table_name(" +
          "review_id BigInt, " +
          "title String, " +
          "deck String, " +
          "lede String, " +
          "body String, " +
          "publish_date Timestamp, " +
          "update_date Timestamp, " +
          "score Double, " +
          "review_type String " +
        ") " +
        "partitioned by (game_id BIGINT, year String) " +
        "clustered by (review_type) " +
        "sorted by (publish_date) " +
        "into 3 buckets " +
        "stored as orc"
    );
  }

  protected def createReviewAuthorsCopy(table_name : String) : Unit = {
    executeDML(connect(),
      "create table if not exists " +
        s"$table_name(" +
        "author_id BigInt " +
        ") " +
        "partitioned by (review_id BigInt) " +
        "stored as orc"
    );
  }

  protected def createAuthorsCopy(table_name : String) : Unit = {
    executeDML(connect(),
      "create table if not exists " +
        s"$table_name(" +
        "author_id BigInt, " +
        "author String" +
        ") " +
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
        result = df.take(1)(0).getInt(0) + 1;

    return result;
  }

  def getQuery(query_id : Int) : String = {
    val df : DataFrame = executeQuery(connect(), s"select query from p1.queries where query_id = $query_id");
    if (!df.isEmpty)
      if (!df.take(1)(0).isNullAt(0))
        return df.take(1)(0).getString(0);
    return "";
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

  def queryExists(query_id: Int): Boolean = {
    val queries : Map[Int, String] = getQueries();
    if (queries.isEmpty)
      return false;
    else
      return queries.keys.exists(q => q.equals(query_id));
  }

  def queryNameExists(query_name : String) : Boolean = {
    val queries : Map[Int, String] = getQueries();
    if (queries.isEmpty)
      return false;
    else
      return queries.values.exists(q => q.equals(query_name));
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

  protected def renameQuery(query_id : Int, newName : String) : Unit = {
    if (!queryNameExists(newName)) {
      val spark: SparkSession = connect();
      var query: (Int, String) = null;
      val df: DataFrame = executeQuery(spark, s"select user_id, query from p1.queries where query_id = $query_id");
      if (!df.isEmpty)
        if (!df.take(1)(0).isNullAt(0))
          query = (
            df.take(1)(0).getInt(0),
            df.take(1)(0).getString(1)
          );
      if (query != null) {
        createQueriesCopy("p1.queriesTemp");
        executeDML(spark, s"insert into p1.queriesTemp select * from p1.queries where query_id != $query_id")
        executeDML(spark, "drop table p1.queries");
        executeDML(spark, "alter table p1.queriesTemp rename to queries");
        executeDML(spark, s"insert into p1.queries values ($query_id, ${query._1}, '$newName', '${query._2}')");
      }
    }
  }

  protected def saveQuery(user_id : Int, query_name : String, query : String) : Unit = {
    executeDML(connect(), s"insert into p1.queries values (${getNextQueryId()}, $user_id, '$query_name', '$query')");
  }

  def exportQueryResults(query_id : Int, filePath : String) : Unit = {
    val df : DataFrame = executeQuery(connect(), getQuery(query_id));
    df.write.option("header", "true").json("temp/");
    val dir : File = new File("temp/");
    for (file : File <- dir.listFiles().filter(f => f.getName.endsWith(".json")).sorted) {
      val writer : FileWriter = new FileWriter(new File(filePath), true);
      val reader : BufferedReader = new BufferedReader(new FileReader(file));
      reader.lines().forEachOrdered(line => writer.write(line + System.lineSeparator()));
      writer.close();
      reader.close();
    }
    dir.listFiles().foreach(f => f.delete());
    dir.delete();
  }

  protected def deleteQuery(query_id : Int) : Unit = {
    val spark : SparkSession = connect();
    createQueriesCopy("p1.queriesTemp");
    executeDML(spark, s"insert into p1.queriesTemp select * from p1.queries where query_id != $query_id");
    executeDML(spark, "drop table p1.queries");
    executeDML(spark, "alter table p1.queriesTemp rename to queries");
  }

  def addGame(game_id : Long, name : String, release_date : LocalDateTime, deck : String, description : String, articles_api_url : String, reviews_api_url : String, article_count : Long, review_count : Long, genres : List[String], themes : List[String]) : Unit = {
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
        s"${article_count}L, " +
        s"${review_count}L "
    );
    for (genre : String <- genres) {
      if (!genreExists(genre))
        addGenre(genre)
      addGameGenre(game_id, getGenreId(genre));
    }
    for (theme : String <- themes) {
      if (!themeExists(theme))
        addGameTheme(game_id, addTheme(theme));
      else
        addGameTheme(game_id, getThemeId(theme));
    }
  }

  def addGames(games : List[(Long, String, LocalDateTime, String, String, String, String, Long, Long, List[String], List[String])]) : Unit = {
    for (game : (Long, String, LocalDateTime, String, String, String, String, Long, Long, List[String], List[String]) <- games)
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
        game._11
      );
  }

  def gameExists(game_id : Long, year : String) : Boolean = {
    val df : DataFrame = executeQuery(connect(), s"select * from p1.games where game_id = ${game_id}L and year = '$year' limit 1");
    return !df.isEmpty;
  }

  def getGame(game_id : Long, year : String) : (Long, String, LocalDateTime, String, String, String, String, Long, Long, List[String], List[String]) = {
    var result : (Long, String, LocalDateTime, String, String, String, String, Long, Long, List[String], List[String]) = null;
    val df : DataFrame = executeQuery(connect(), s"select game_id, name, release_date, deck, description, articles_api_url, reviews_api_url, article_count, review_count from p1.games where game_id = ${game_id}L and year = '$year' limit 1");
    if (!df.isEmpty) {
      val row : Row = df.take(1)(0);
      result = (
        row.getLong(0),
        row.getString(1),
        row.getTimestamp(2).toLocalDateTime,
        row.getString(3),
        row.getString(4),
        row.getString(5),
        row.getString(6),
        row.getLong(7),
        row.getLong(8),
        getGameGenres(game_id),
        getGameThemes(game_id)
      );
    }
    return result;
  }

  def getGame(game_name : String) : (Long, String, LocalDateTime, String, String, String, String, Long, Long, List[String], List[String]) = {
    var result : (Long, String, LocalDateTime, String, String, String, String, Long, Long, List[String], List[String]) = null;
    val df : DataFrame = executeQuery(connect(), s"select game_id, name, release_date, deck, description, articles_api_url, reviews_api_url, article_count, review_count from p1.games where name = '$game_name' limit 1");
    if (!df.isEmpty) {
      val row : Row = df.take(1)(0);
      result = (
        row.getLong(0),
        row.getString(1),
        row.getTimestamp(2).toLocalDateTime,
        row.getString(3),
        row.getString(4),
        row.getString(5),
        row.getString(6),
        row.getLong(7),
        row.getLong(8),
        getGameGenres(row.getLong(0)),
        getGameThemes(row.getLong(0))
      );
    }
    return result;
  }

  def getGamesBetween(startDate : LocalDateTime, endDate : LocalDateTime) : List[(Long, String, LocalDateTime, String, String, String, String, Long, Long, List[String], List[String])] = {
    var result : ArrayBuffer[(Long, String, LocalDateTime, String, String, String, String, Long, Long, List[String], List[String])] = ArrayBuffer();
    val df : DataFrame = executeQuery(connect(), s"select game_id, name, release_date, deck, description, articles_api_url, reviews_api_url, article_count, review_count from p1.games where release_date between '${Timestamp.valueOf(startDate)}' and '${Timestamp.valueOf(endDate)}' and year between '${startDate.getYear}' and '${endDate.getYear}' order by game_id")
    if (!df.isEmpty)
      if (!df.take(1)(0).isNullAt(0))
        for(row : Row <- df.collect()) {
          val tuple : (Long, String, LocalDateTime, String, String, String, String, Long, Long, List[String], List[String]) = (
            row.getLong(0),
            row.getString(1),
            row.getTimestamp(2).toLocalDateTime,
            row.getString(3),
            row.getString(4),
            row.getString(5),
            row.getString(6),
            row.getLong(7),
            row.getLong(8),
            getGameGenres(row.getLong(0)),
            getGameThemes(row.getLong(0))
          );
          result += tuple;
        }

    return result.toList;
  }

  def getMaxGameBetween(startDate : LocalDateTime, endDate : LocalDateTime) : (Long, String, LocalDateTime, String, String, String, String, Long, Long, List[String], List[String]) = {
    var result : (Long, String, LocalDateTime, String, String, String, String, Long, Long, List[String], List[String]) = null;
    val df : DataFrame = executeQuery(connect(), s"select max(game_id), year from p1.games where release_date between '${Timestamp.valueOf(startDate)}' and '${Timestamp.valueOf(endDate)}' and year between '${startDate.getYear}' and '${endDate.getYear}'");
    if (!df.isEmpty)
      if (!df.take(1)(0).isNullAt(0))
        result = getGame(
          df.take(1)(0).getLong(0),
          df.take(1)(0).getString(1)
        );

    return result;
  }

  def getGameCountBetween(startDate : LocalDateTime, endDate : LocalDateTime) : Long = {
    var result : Long = 0L;
    val df : DataFrame = executeQuery(connect(), s"select count(*) from p1.games where release_date between '${Timestamp.valueOf(startDate)}' and '${Timestamp.valueOf(endDate)}' and year between '${startDate.getYear}' and '${endDate.getYear}'");
    if (!df.isEmpty)
      if (!df.take(1)(0).isNullAt(0))
        result = df.take(1)(0).getLong(0);

    return result;
  }

  def getGames() : List[(Long, String, LocalDateTime, String, String, String, String, Long, Long, List[String], List[String])] = {
    var results : ArrayBuffer[(Long, String, LocalDateTime, String, String, String, String, Long, Long, List[String], List[String])] = ArrayBuffer();
    val df : DataFrame = executeQuery(connect(), s"select game_id, name, release_date, deck, description, articles_api_url, reviews_api_url, article_count, review_count from p1.games order by game_id");
    if (!df.isEmpty)
      if (!df.take(1)(0).isNullAt(0))
        for (row: Row <- df.collect()) {
          val tuple: (Long, String, LocalDateTime, String, String, String, String, Long, Long, List[String], List[String]) = (
            row.getLong(0),
            row.getString(1),
            row.getTimestamp(2).toLocalDateTime,
            row.getString(3),
            row.getString(4),
            row.getString(5),
            row.getString(6),
            row.getLong(7),
            row.getLong(8),
            getGameGenres(row.getLong(0)),
            getGameThemes(row.getLong(0))
          );
          results += tuple;
        }

    return results.toList;
  }

  def getMaxGame() : (Long, String, LocalDateTime, String, String, String, String, Long, Long, List[String], List[String]) = {
    val df : DataFrame = executeQuery(connect(), s"select max(game_id), year from p1.games");
    if (!df.isEmpty)
      if (!df.take(1)(0).isNullAt(0))
        return getGame(df.take(1)(0).getLong(0), df.take(1)(0).getString(1));

    return null;
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

  def getPreviousGameArticleCount(game_id : Long, year : String) : Long = {
    var result : Long = Long.MinValue;
    val game : (Long, String, LocalDateTime, String, String, String, String, Long, Long, List[String], List[String]) = getGame(game_id, year);
    if (game != null)
      result = game._8;
    return result;
  }

  def updateArticleCount(game_id : Long, year : String, newArticleCount : Long) : Long = {
    var result : Long = Long.MinValue;
    val game : (Long, String, LocalDateTime, String, String, String, String, Long, Long, List[String], List[String]) = getGame(game_id, year);
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
          newArticleCount,
          game._9,
          game._10,
          game._11
        );
      }
    }
    return result;
  }

  def getPreviousGameReviewCount(game_id : Long, year : String) : Long = {
    var result : Long = Long.MinValue;
    val game : (Long, String, LocalDateTime, String, String, String, String, Long, Long, List[String], List[String]) = getGame(game_id, year);
    if (game != null)
      result = game._9
    return result;
  }

  def updateReviewCount(game_id : Long, year : String, newReviewCount : Long) : Long = {
    var result : Long = Long.MinValue;
    val game : (Long, String, LocalDateTime, String, String, String, String, Long, Long, List[String], List[String]) = getGame(game_id, year);
    if (game != null) {
      result = game._9
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
          newReviewCount,
          game._10,
          game._11
        );
      }
    }
    return result;
  }

  def deleteGame(game_id : Long, year : String) : (Long, String, LocalDateTime, String, String, String, String, Long, Long, List[String], List[String]) = {
    val result : (Long, String, LocalDateTime, String, String, String, String, Long, Long, List[String], List[String]) = getGame(game_id, year);
    if (result != null) {
      val spark: SparkSession = connect();
      createGamesCopy("p1.gamesTemp");
      executeDML(spark, s"insert into p1.gamesTemp select * from p1.games where game_id != ${game_id}L");
      executeDML(spark, "drop table p1.games");
      executeDML(spark, "alter table p1.gamesTemp rename to games");
    }
    return result;
  }

  def deleteGames(games : Map[Long, String]) : List[(Long, String, LocalDateTime, String, String, String, String, Long, Long, List[String], List[String])] = {
    val result : ArrayBuffer[(Long, String, LocalDateTime, String, String, String, String, Long, Long, List[String], List[String])] = ArrayBuffer();
    for (game_id : Long <- games.keys) {
      val game : (Long, String, LocalDateTime, String, String, String, String, Long, Long, List[String], List[String]) = deleteGame(game_id, games(game_id));
      if (game != null)
        result += game;
    }
    return result.toList;
  }

  def addReview(review_id : Long, authors : String, title : String, deck : String, lede : String, body : String, publish_date : LocalDateTime, update_date : LocalDateTime, score : Double, review_type : String, game_id : Long) : Unit = {
    val publishDate : String = publish_date.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    val updateDate : String = update_date.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    executeDML(connect(),
      s"insert into p1.reviews partition(game_id=$game_id, year='${publish_date.getYear}') select " +
        s"${review_id}L, " +
        s"'$title', " +
        s"'$deck', " +
        s"'$lede', " +
        s"'$body', " +
        s"to_timestamp('$publishDate'), " +
        s"to_timestamp('$updateDate'), " +
        s"$score, " +
        s"'$review_type'"
    );
    for (author : String <- authors.split("; "))
      if (!authorExists(author))
        addReviewAuthor(review_id, addAuthor(author));
      else
        addReviewAuthor(review_id, getAuthorId(author));
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

  def reviewExists(review_id : Long, year : String) : Boolean = {
    val df : DataFrame = executeQuery(connect(), s"select * from p1.reviews where review_id = ${review_id}L and year = '$year' limit 1");
    return !df.isEmpty;
  }

  def getReview(review_id : Long, year : String) : (Long, String, String, String, String, String, LocalDateTime, LocalDateTime, Double, String, Long) = {
    var result : (Long, String, String, String, String, String, LocalDateTime, LocalDateTime, Double, String, Long) = null;
    val df : DataFrame = executeQuery(connect(), s"select review_id, title, deck, lede, body, publish_date, update_date, score, review_type, game_id from p1.reviews where review_id = ${review_id}L and year = '$year' limit 1");
    if (!df.isEmpty)
      if (!df.take(1)(0).isNullAt(0)) {
        val row : Row = df.take(1)(0);
        val tuple : (Long, String, String, String, String, String, LocalDateTime, LocalDateTime, Double, String, Long) = (
          row.getLong(0),
          getReviewAuthors(review_id),
          row.getString(1),
          row.getString(2),
          row.getString(3),
          row.getString(4),
          row.getTimestamp(5).toLocalDateTime,
          row.getTimestamp(6).toLocalDateTime,
          row.getDouble(7),
          row.getString(8),
          row.getLong(9)
        );
        result = tuple;
      }

    return result;
  }

  def getGameReviews(game_id : Long) : List[(Long, String, String, String, String, String, LocalDateTime, LocalDateTime, Double, String, Long)] = {
    var result : ArrayBuffer[(Long, String, String, String, String, String, LocalDateTime, LocalDateTime, Double, String, Long)] = ArrayBuffer();
    val df : DataFrame = executeQuery(connect(), s"select review_id, title, deck, lede, body, publish_date, update_date, score, review_type, game_id from p1.reviews where game_id = ${game_id}L");
    if (!df.isEmpty)
      if (!df.take(1)(0).isNullAt(0))
        for (row : Row <- df.collect()) {
          val tuple : (Long, String, String, String, String, String, LocalDateTime, LocalDateTime, Double, String, Long) = (
            row.getLong(0),
            getReviewAuthors(row.getLong(0)),
            row.getString(1),
            row.getString(2),
            row.getString(3),
            row.getString(4),
            row.getTimestamp(5).toLocalDateTime,
            row.getTimestamp(6).toLocalDateTime,
            row.getDouble(7),
            row.getString(8),
            row.getLong(9)
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
    for (review : (Long, String, String, String, String, String, LocalDateTime, LocalDateTime, Double, String, Long) <- result)
      deleteReviewAuthors(review._1);

    return result;
  }

  def addArticle(article_id : Long, authors : String, title : String, deck : String, lede : String, body : String, publish_date : LocalDateTime, update_date : LocalDateTime, categories : Map[Long, String], game_id : Long) : Unit = {
    val publishDate : String = publish_date.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    val updateDate : String = update_date.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

    executeDML(connect(),
      s"insert into p1.articles partition(game_id=$game_id, year='${publish_date.getYear}') select " +
        s"${article_id}L, " +
        s"'$title', " +
        s"'$deck', " +
        s"'$lede', " +
        s"'$body', " +
        s"to_timestamp('$publishDate'), " +
        s"to_timestamp('$updateDate'), "
    );
    authors.split("; ").foreach(a => {
      if (!authorExists(a))
        addArticleAuthor(article_id, addAuthor(a));
      else
        addArticleAuthor(article_id, getAuthorId(a));
    });
    for (category : (Long, String) <- categories) {
      if (!categoryExists(category._1))
        addCategory(category._1, category._2);
      addArticleCategory(article_id, category._1);
    };
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

  def articleExists(article_id : Long, year : String) : Boolean = {
    val df : DataFrame = executeQuery(connect(), s"select * from p1.articles where article_id = ${article_id}L and year = '$year' limit 1");
   return !df.isEmpty;
  }

  def getArticle(article_id : Long, year : String) : (Long, String, String, String, String, String, LocalDateTime, LocalDateTime, Map[Long, String], Long) = {
    var result : (Long, String, String, String, String, String, LocalDateTime, LocalDateTime, Map[Long, String], Long) = null;
    val df : DataFrame = executeQuery(connect(), s"select article_id, title, deck, lede, body, publish_date, update_date, game_id from p1.articles where article_id = ${article_id}L and year = '$year' limit 1");
    if (!df.isEmpty)
      if (!df.take(1)(0).isNullAt(0)) {
        val row : Row = df.take(1)(0);
        val categories : Map[Long, String] = row.get(8).asInstanceOf[Map[Long, String]];
        val tuple : (Long, String, String, String, String, String, LocalDateTime, LocalDateTime, Map[Long, String], Long) = (
          row.getLong(0),
          getArticleAuthors(row.getLong(0)),
          row.getString(1),
          row.getString(2),
          row.getString(3),
          row.getString(4),
          row.getTimestamp(5).toLocalDateTime,
          row.getTimestamp(6).toLocalDateTime,
          getArticleCategories(row.getLong(0)),
          row.getLong(7)
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
    val df : DataFrame = executeQuery(connect(), s"select article_id, title, deck, lede, body, publish_date, update_date, game_id from p1.articles where game_id = ${game_id}L");
    if (!df.isEmpty)
      if (!df.take(1)(0).isNullAt(0))
        for (row : Row <- df.collect()) {
          val tuple : (Long, String, String, String, String, String, LocalDateTime, LocalDateTime, Map[Long, String], Long) = (
            row.getLong(0),
            getArticleAuthors(row.getLong(0)),
            row.getString(1),
            row.getString(2),
            row.getString(3),
            row.getString(4),
            row.getTimestamp(5).toLocalDateTime,
            row.getTimestamp(6).toLocalDateTime,
            getArticleCategories(row.getLong(0)),
            row.getLong(7)
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
    for (article : (Long, String, String, String, String, String, LocalDateTime, LocalDateTime, Map[Long, String], Long) <- result) {
      deleteArticleAuthors(article._1);
      deleteArticleCategories(article._1);
    }

    return result;
  }

  protected def addAuthor(author : String) : Long = {
    val result : Long = getNextAuthorId();
    executeDML(connect(), s"insert into p1.authors values ($result, '$author')");
    return result;
  }

  protected def authorExists(author : String) : Boolean = {
    val df : DataFrame = executeQuery(connect(), s"select * from p1.authors where author = '$author' limit 1");
    return !df.isEmpty;
  }

  protected def getAuthorId(author : String) : Long = {
    var result : Long = 0L;
    val df : DataFrame = executeQuery(connect(), s"select author_id from p1.authors where author = '$author' limit 1");
    if (!df.isEmpty)
      result = df.take(1)(0).getLong(0);
    return result;
  }

  protected def getNextAuthorId() : Long = {
    val df : DataFrame = executeQuery(connect(), "select max(author_id) from p1.authors");
    if (!df.isEmpty)
      if (!df.take(1)(0).isNullAt(0))
        return df.take(1)(0).getLong(0) + 1;
    return 1L;
  }

  protected def addArticleAuthor(article_id : Long, author_id : Long) : Unit = {
    executeDML(connect(), s"insert into p1.articleAuthors values ($article_id, $author_id)");
  }

  protected def getArticleAuthors(article_id : Long) : String = {
    var result : String = "";
    val df : DataFrame = executeQuery(connect(), s"select au.author from p1.authors au, p1.articleAuthors aa, p1.articles a where a.article_id = $article_id and a.article_id = aa.article_id and aa.author_id = au.author_id order by au.author");
    if (!df.isEmpty) {
      var authors : ArrayBuffer[String] = ArrayBuffer();
      for (row : Row <- df.collect())
        authors += row.getString(0);
      result = authors.reduce((x, y) => x + "; " + y);
    }
    return result;
  }

  protected def deleteArticleAuthors(article_id : Long) : String = {
    val result : String = getArticleAuthors(article_id);
    if (result.nonEmpty) {
      val spark : SparkSession = connect();
      createArticleAuthorsCopy("p1.articleAuthorsTemp");
      executeDML(spark, s"insert into p1.articleAuthorsTemp select * from p1.articleAuthors where article_id != $article_id");
      executeDML(spark, "drop table p1.articleAuthors");
      executeDML(spark, "alter table p1.articleAuthorsTemp rename to articleAuthors");
    }

    return result;
  }

  protected def addReviewAuthor(review_id : Long, author_id : Long) : Unit = {
    executeDML(connect(), s"insert into p1.reviewAuthors values ($review_id, $author_id)");
  }

  protected def getReviewAuthors(review_id : Long) : String = {
    var result : String = "";
    val df : DataFrame = executeQuery(connect(), s"select a.author from p1.authors a, p1.reviewAuthors ra, p1.reviews r where r.review_id = $review_id and r.review_id = ra.review_id and ra.author_id = a.author_id order by a.author");
    if (!df.isEmpty) {
      var authors : ArrayBuffer[String] = ArrayBuffer();
      for (row : Row <- df.collect())
        authors += row.getString(0);
      result = authors.reduce((x, y) => x + "; " + y);
    }
    return result;
  }

  protected def deleteReviewAuthors(review_id : Long) : String = {
    val result : String = getReviewAuthors(review_id);
    if (result.nonEmpty) {
      val spark : SparkSession = connect();
      createReviewAuthorsCopy("p1.reviewAuthorsTemp");
      executeDML(spark, s"insert into p1.reviewAuthorsTemp select * from p1.reviewAuthors where review_id != $review_id");
      executeDML(spark, "drop table p1.reviewAuthors");
      executeDML(spark, "alter table p1.reviewAuthorsTemp rename to reviewAuthors");
    }

    return result;
  }

  protected def addCategory(category_id : Long, category : String) : Unit = {
    executeDML(connect(), s"insert into p1.categories values ($category_id, '$category')");
  }

  protected def categoryExists(category_id : Long) : Boolean = {
    val df : DataFrame = executeQuery(connect(), s"select * from p1.categories where category_id = $category_id");
    return !df.isEmpty;
  }

  protected def addArticleCategory(article_id : Long, category_id : Long) : Unit = {
    executeDML(connect(), s"insert into p1.articleCategory values ($article_id, $category_id)");
  }

  protected def getArticleCategories(article_id : Long) : Map[Long, String] = {
    var result : Map[Long, String] = Map();
    val df : DataFrame = executeQuery(connect(), s"select c.category_id, c.category from p1.categories c, p1.articleCategories ac, p1.articles a where a.article_id = $article_id and a.article_id = ac.article_id and ac.category_id = c.category_id order by c.category_id");
    if (!df.isEmpty)
      for(row : Row <- df.collect())
        result += (row.getLong(0) -> row.getString(1));
    return result;
  }

  protected def deleteArticleCategories(article_id : Long) : Map[Long, String] = {
    val result : Map[Long, String] = getArticleCategories(article_id);
    if (result.nonEmpty) {
      val spark : SparkSession = connect();
      createArticleCategoriesCopy("p1.articleCategoriesTemp");
      executeDML(spark, s"insert into p1.articleCategoriesTemp select * from p1.articleCategories where article_id != $article_id");
      executeDML(spark, "drop table p1.articleCategories");
      executeDML(spark, "alter table p1.articleCategoriesTemp rename to articleCategories");
    }

    return result;
  }

  def addGenre(genre : String) : Long = {
    val result : Long = getNextGenreId();
    executeDML(connect(), s"insert into p1.genres values ($result, '$genre')");
    return result;
  }

  def genreExists(genre : String) : Boolean = {
    val df : DataFrame = executeQuery(connect(), s"select * from p1.genres where genre = '$genre' limit 1");
    return !df.isEmpty;
  }

  def getGenreId(genre : String) : Long = {
    val df : DataFrame = executeQuery(connect(), s"select genre_id from p1.genres where genre = '$genre' limit 1");
    if (!df.isEmpty)
      return df.take(1)(0).getLong(0);
    return 0L;
  }

  protected def getNextGenreId() : Long = {
    val df : DataFrame = executeQuery(connect(), s"select max(genre_id) from p1.genres");
    if (!df.isEmpty) {
      if (!df.take(1)(0).isNullAt(0))
      return df.take(1)(0).getLong(0) + 1;
    };
    return 1L;
  }

  protected def addGameGenre(game_id : Long, genre_id : Long) : Unit = {
    executeDML(connect(), s"insert into p1.gameGenres values ($game_id, $genre_id)");
  }

  protected def getGameGenres(game_id : Long) : List[String] = {
    var result : ArrayBuffer[String] = ArrayBuffer();
    val df : DataFrame = executeQuery(connect(), s"select ge.genre from p1.genres ge, p1.gameGenres gg, p1.games g where g.game_id = $game_id and g.game_id = gg.game_id and gg.genre_id = ge.genre_id order by ge.genre_id");
    if (!df.isEmpty)
      for(row : Row <- df.collect())
        result += row.getString(0);
    return result.toList;;
  }

  protected def deleteGameGenres(game_id : Long) : List[String] = {
    val result : List[String] = getGameGenres(game_id);
    if (result.nonEmpty) {
      val spark : SparkSession = connect();
      createGameGenresCopy("p1.gameGenresTemp");
      executeDML(spark, s"insert into p1.gameGenresTemp select * from p1.gameGenres where game_id != $game_id");
      executeDML(spark, "drop table p1.gameGenres");
      executeDML(spark, "alter table p1.gameGenresTemp rename to gameGenres");
    }
    return result;
  }

  def addTheme(theme : String) : Long = {
    val result : Long = getNextThemeId();
    executeDML(connect(), s"insert into p1.themes values ($result, '$theme')");
    return result;
  }

  def themeExists(theme : String) : Boolean = {
    val df : DataFrame = executeQuery(connect(), s"select * from p1.themes where theme = '$theme' limit 1");
    return !df.isEmpty;
  }

  def getThemeId(theme : String) : Long = {
    val df : DataFrame = executeQuery(connect(), s"select theme_id from p1.themes where theme = '$theme' limit 1");
    if (!df.isEmpty)
      return df.take(1)(0).getLong(0);
    return 0L;
  }

  protected def getNextThemeId() : Long = {
    val df : DataFrame = executeQuery(connect(), s"select max(theme_id) from p1.themes");
    if (!df.isEmpty)
      if (!df.take(1)(0).isNullAt(0))
        return df.take(1)(0).getLong(0) + 1;
    return 1L;
  }

  protected def addGameTheme(game_id : Long, theme_id : Long) : Unit = {
    executeDML(connect(), s"insert into p1.gameThemes values ($game_id, $theme_id)");
  }

  protected def getGameThemes(game_id : Long) : List[String] = {
    var result : ArrayBuffer[String] = ArrayBuffer();
    val df : DataFrame = executeQuery(connect(), s"select t.theme from p1.themes t, p1.gameThemes gt, p1.games g where g.game_id = $game_id and g.game_id = gt.game_id and gt.theme_id = t.theme_id order by t.theme_id");
    if (!df.isEmpty)
      for (row : Row <- df.collect())
        result += row.getString(0);
    return result.toList;
  }

  protected def deleteGameThemes(game_id : Long) : List[String] = {
    val result : List[String] = getGameThemes(game_id);
    if (result.nonEmpty) {
      val spark : SparkSession = connect();
      createGameThemesCopy("p1.gameThemesTemp");
      executeDML(spark, s"insert into p1.gameThemesTemp select * from p1.gameThemes where game_id != $game_id");
      executeDML(spark, "drop table p1.gameThemes");
      executeDML(spark, "alter table p1.gameThemesTemp rename to gameThemes");
    }
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
    createGameGenresCopy("p1.gameGenres");
    createGenresCopy("p1.genres");
    createGameThemesCopy("p1.gameThemes");
    createThemesCopy("p1.themes");
    createReviewsCopy("p1.reviews");
    createReviewAuthorsCopy("p1.reviewAuthors");
    createArticlesCopy("p1.articles");
    createArticleAuthorsCopy("p1.articleAuthors");
    createAuthorsCopy("p1.authors");
    createArticleCategoriesCopy("p1.articleCategories");
    createCategoriesCopy("p1.categories");

    executeDML(spark, s"insert into p1.users values (${getNextUserId()}, 'admin', '${"admin".sha512.hex}', true)");
  }

  def startupDB() : Unit = {
    val spark : SparkSession = connect();
  }

  protected def createUsersCopy(table_name : String) : Unit = {
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

  protected def createQueriesCopy(table_name : String) : Unit = {
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

  protected def createGamesCopy(table_name : String) : Unit = {
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
        "article_count BigInt, " +
        "review_count BigInt" +
        ") " +
        "partitioned by (year String) " +
        "clustered by (year) " +
        "sorted by (release_date) " +
        "into 50 buckets " +
        "stored as orc"
    );
  }

  protected def createGenresCopy(table_name : String) : Unit = {
    executeDML(connect(),
      "create table if not exists " +
        s"$table_name(" +
        "genre_id BigInt, " +
        "genre String" +
        ") " +
        "stored as orc"
    );
  }

  protected def createGameGenresCopy(table_name : String) : Unit = {
    executeDML(connect(),
      "create table if not exists " +
        s"$table_name(" +
        "genre_id BigInt " +
        ") " +
        "partitioned by (game_id BigInt) " +
        "stored as orc"
    );
  }

  protected def createThemesCopy(table_name : String) : Unit = {
    executeDML(connect(),
      "create table if not exists " +
        s"$table_name(" +
        "theme_id BigInt, " +
        "theme String" +
        ") " +
        "stored as orc"
    );
  }

  protected def createGameThemesCopy(table_name : String) : Unit = {
    executeDML(connect(),
      "create table if not exists " +
        s"$table_name(" +
        "theme_id BigInt " +
        ") " +
        "partitioned by (game_id BigInt) " +
        "stored as orc"
    );
  }

  protected def createArticlesCopy(table_name : String) : Unit = {
    executeDML(connect(),
      "create table if not exists " +
        s"$table_name(" +
        "article_id BigInt, " +
        "title String, " +
        "deck String, " +
        "lede String, " +
        "body String, " +
        "publish_date Timestamp, " +
        "update_date Timestamp " +
        ") " +
        "partitioned by (game_id BigInt, year String) " +
        "stored as orc"
    );
  }

  protected def createCategoriesCopy(table_name : String) : Unit = {
    executeDML(connect(),
      "create table if not exists " +
        s"$table_name(" +
        "category_id BigInt, " +
        "category String" +
        ") " +
        "stored as orc"
    );
  }

  protected def createArticleCategoriesCopy(table_name : String) : Unit = {
    executeDML(connect(),
      "create table if not exists " +
        s"$table_name(" +
        "category_id BigInt " +
        ") " +
        "partitioned by (article_id BigInt) " +
        "stored as orc"
    );
  }

  protected def createArticleAuthorsCopy(table_name : String) : Unit = {
    executeDML(connect(),
      "create table if not exists " +
        s"$table_name(" +
        "author_id BigInt " +
        ") " +
        "partitioned by (article_id BigInt) " +
        "stored as orc"
    );
  }

  protected def createReviewsCopy(table_name : String) : Unit = {
    executeDML(connect(),
      "create table if not exists " +
        s"$table_name(" +
        "review_id BigInt, " +
        "title String, " +
        "deck String, " +
        "lede String, " +
        "body String, " +
        "publish_date Timestamp, " +
        "update_date Timestamp, " +
        "score Double, " +
        "review_type String " +
        ") " +
        "partitioned by (game_id BIGINT, year String) " +
        "clustered by (review_type) " +
        "sorted by (publish_date) " +
        "into 3 buckets " +
        "stored as orc"
    );
  }

  protected def createReviewAuthorsCopy(table_name : String) : Unit = {
    executeDML(connect(),
      "create table if not exists " +
        s"$table_name(" +
        "author_id BigInt " +
        ") " +
        "partitioned by (review_id BigInt) " +
        "stored as orc"
    );
  }

  protected def createAuthorsCopy(table_name : String) : Unit = {
    executeDML(connect(),
      "create table if not exists " +
        s"$table_name(" +
        "author_id BigInt, " +
        "author String" +
        ") " +
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
        result = df.take(1)(0).getInt(0) + 1;

    return result;
  }

  def getQuery(query_id : Int) : String = {
    val df : DataFrame = executeQuery(connect(), s"select query from p1.queries where query_id = $query_id");
    if (!df.isEmpty)
      if (!df.take(1)(0).isNullAt(0))
        return df.take(1)(0).getString(0);
    return "";
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

  def queryExists(query_id: Int): Boolean = {
    val queries : Map[Int, String] = getQueries();
    if (queries.isEmpty)
      return false;
    else
      return queries.keys.exists(q => q.equals(query_id));
  }

  def queryNameExists(query_name : String) : Boolean = {
    val queries : Map[Int, String] = getQueries();
    if (queries.isEmpty)
      return false;
    else
      return queries.values.exists(q => q.equals(query_name));
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

  protected def renameQuery(query_id : Int, newName : String) : Unit = {
    if (!queryNameExists(newName)) {
      val spark: SparkSession = connect();
      var query: (Int, String) = null;
      val df: DataFrame = executeQuery(spark, s"select user_id, query from p1.queries where query_id = $query_id");
      if (!df.isEmpty)
        if (!df.take(1)(0).isNullAt(0))
          query = (
            df.take(1)(0).getInt(0),
            df.take(1)(0).getString(1)
          );
      if (query != null) {
        createQueriesCopy("p1.queriesTemp");
        executeDML(spark, s"insert into p1.queriesTemp select * from p1.queries where query_id != $query_id")
        executeDML(spark, "drop table p1.queries");
        executeDML(spark, "alter table p1.queriesTemp rename to queries");
        executeDML(spark, s"insert into p1.queries values ($query_id, ${query._1}, '$newName', '${query._2}')");
      }
    }
  }

  protected def saveQuery(user_id : Int, query_name : String, query : String) : Unit = {
    executeDML(connect(), s"insert into p1.queries values (${getNextQueryId()}, $user_id, '$query_name', '$query')");
  }

  def exportQueryResults(query_id : Int, filePath : String) : Unit = {
    val df : DataFrame = executeQuery(connect(), getQuery(query_id));
    df.write.option("header", "true").json("temp/");
    val dir : File = new File("temp/");
    for (file : File <- dir.listFiles().filter(f => f.getName.endsWith(".json")).sorted) {
      val writer : FileWriter = new FileWriter(new File(filePath), true);
      val reader : BufferedReader = new BufferedReader(new FileReader(file));
      reader.lines().forEachOrdered(line => writer.write(line + System.lineSeparator()));
      writer.close();
      reader.close();
    }
    dir.listFiles().foreach(f => f.delete());
    dir.delete();
  }

  protected def deleteQuery(query_id : Int) : Unit = {
    val spark : SparkSession = connect();
    createQueriesCopy("p1.queriesTemp");
    executeDML(spark, s"insert into p1.queriesTemp select * from p1.queries where query_id != $query_id");
    executeDML(spark, "drop table p1.queries");
    executeDML(spark, "alter table p1.queriesTemp rename to queries");
  }

  def addGame(game_id : Long, name : String, release_date : LocalDateTime, deck : String, description : String, articles_api_url : String, reviews_api_url : String, article_count : Long, review_count : Long, genres : List[String], themes : List[String]) : Unit = {
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
        s"${article_count}L, " +
        s"${review_count}L "
    );
    for (genre : String <- genres) {
      if (!genreExists(genre))
        addGenre(genre)
      addGameGenre(game_id, getGenreId(genre));
    }
    for (theme : String <- themes) {
      if (!themeExists(theme))
        addGameTheme(game_id, addTheme(theme));
      else
        addGameTheme(game_id, getThemeId(theme));
    }
  }

  def addGames(games : List[(Long, String, LocalDateTime, String, String, String, String, Long, Long, List[String], List[String])]) : Unit = {
    for (game : (Long, String, LocalDateTime, String, String, String, String, Long, Long, List[String], List[String]) <- games)
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
        game._11
      );
  }

  def gameExists(game_id : Long, year : String) : Boolean = {
    val df : DataFrame = executeQuery(connect(), s"select * from p1.games where game_id = ${game_id}L and year = '$year' limit 1");
    return !df.isEmpty;
  }

  def getGame(game_id : Long, year : String) : (Long, String, LocalDateTime, String, String, String, String, Long, Long, List[String], List[String]) = {
    var result : (Long, String, LocalDateTime, String, String, String, String, Long, Long, List[String], List[String]) = null;
    val df : DataFrame = executeQuery(connect(), s"select game_id, name, release_date, deck, description, articles_api_url, reviews_api_url, article_count, review_count from p1.games where game_id = ${game_id}L and year = '$year' limit 1");
    if (!df.isEmpty) {
      val row : Row = df.take(1)(0);
      result = (
        row.getLong(0),
        row.getString(1),
        row.getTimestamp(2).toLocalDateTime,
        row.getString(3),
        row.getString(4),
        row.getString(5),
        row.getString(6),
        row.getLong(7),
        row.getLong(8),
        getGameGenres(game_id),
        getGameThemes(game_id)
      );
    }
    return result;
  }

  def getGame(game_name : String) : (Long, String, LocalDateTime, String, String, String, String, Long, Long, List[String], List[String]) = {
    var result : (Long, String, LocalDateTime, String, String, String, String, Long, Long, List[String], List[String]) = null;
    val df : DataFrame = executeQuery(connect(), s"select game_id, name, release_date, deck, description, articles_api_url, reviews_api_url, article_count, review_count from p1.games where name = '$game_name' limit 1");
    if (!df.isEmpty) {
      val row : Row = df.take(1)(0);
      result = (
        row.getLong(0),
        row.getString(1),
        row.getTimestamp(2).toLocalDateTime,
        row.getString(3),
        row.getString(4),
        row.getString(5),
        row.getString(6),
        row.getLong(7),
        row.getLong(8),
        getGameGenres(row.getLong(0)),
        getGameThemes(row.getLong(0))
      );
    }
    return result;
  }

  def getGamesBetween(startDate : LocalDateTime, endDate : LocalDateTime) : List[(Long, String, LocalDateTime, String, String, String, String, Long, Long, List[String], List[String])] = {
    var result : ArrayBuffer[(Long, String, LocalDateTime, String, String, String, String, Long, Long, List[String], List[String])] = ArrayBuffer();
    val df : DataFrame = executeQuery(connect(), s"select game_id, name, release_date, deck, description, articles_api_url, reviews_api_url, article_count, review_count from p1.games where release_date between '${Timestamp.valueOf(startDate)}' and '${Timestamp.valueOf(endDate)}' and year between '${startDate.getYear}' and '${endDate.getYear}' order by game_id")
    if (!df.isEmpty)
      if (!df.take(1)(0).isNullAt(0))
        for(row : Row <- df.collect()) {
          val tuple : (Long, String, LocalDateTime, String, String, String, String, Long, Long, List[String], List[String]) = (
            row.getLong(0),
            row.getString(1),
            row.getTimestamp(2).toLocalDateTime,
            row.getString(3),
            row.getString(4),
            row.getString(5),
            row.getString(6),
            row.getLong(7),
            row.getLong(8),
            getGameGenres(row.getLong(0)),
            getGameThemes(row.getLong(0))
          );
          result += tuple;
        }

    return result.toList;
  }

  def getMaxGameBetween(startDate : LocalDateTime, endDate : LocalDateTime) : (Long, String, LocalDateTime, String, String, String, String, Long, Long, List[String], List[String]) = {
    var result : (Long, String, LocalDateTime, String, String, String, String, Long, Long, List[String], List[String]) = null;
    val df : DataFrame = executeQuery(connect(), s"select max(game_id), year from p1.games where release_date between '${Timestamp.valueOf(startDate)}' and '${Timestamp.valueOf(endDate)}' and year between '${startDate.getYear}' and '${endDate.getYear}'");
    if (!df.isEmpty)
      if (!df.take(1)(0).isNullAt(0))
        result = getGame(
          df.take(1)(0).getLong(0),
          df.take(1)(0).getString(1)
        );

    return result;
  }

  def getGameCountBetween(startDate : LocalDateTime, endDate : LocalDateTime) : Long = {
    var result : Long = 0L;
    val df : DataFrame = executeQuery(connect(), s"select count(*) from p1.games where release_date between '${Timestamp.valueOf(startDate)}' and '${Timestamp.valueOf(endDate)}' and year between '${startDate.getYear}' and '${endDate.getYear}'");
    if (!df.isEmpty)
      if (!df.take(1)(0).isNullAt(0))
        result = df.take(1)(0).getLong(0);

    return result;
  }

  def getGames() : List[(Long, String, LocalDateTime, String, String, String, String, Long, Long, List[String], List[String])] = {
    var results : ArrayBuffer[(Long, String, LocalDateTime, String, String, String, String, Long, Long, List[String], List[String])] = ArrayBuffer();
    val df : DataFrame = executeQuery(connect(), s"select game_id, name, release_date, deck, description, articles_api_url, reviews_api_url, article_count, review_count from p1.games order by game_id");
    if (!df.isEmpty)
      if (!df.take(1)(0).isNullAt(0))
        for (row: Row <- df.collect()) {
          val tuple: (Long, String, LocalDateTime, String, String, String, String, Long, Long, List[String], List[String]) = (
            row.getLong(0),
            row.getString(1),
            row.getTimestamp(2).toLocalDateTime,
            row.getString(3),
            row.getString(4),
            row.getString(5),
            row.getString(6),
            row.getLong(7),
            row.getLong(8),
            getGameGenres(row.getLong(0)),
            getGameThemes(row.getLong(0))
          );
          results += tuple;
        }

    return results.toList;
  }

  def getMaxGame() : (Long, String, LocalDateTime, String, String, String, String, Long, Long, List[String], List[String]) = {
    val df : DataFrame = executeQuery(connect(), s"select max(game_id), year from p1.games");
    if (!df.isEmpty)
      if (!df.take(1)(0).isNullAt(0))
        return getGame(df.take(1)(0).getLong(0), df.take(1)(0).getString(1));

    return null;
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

  def getPreviousGameArticleCount(game_id : Long, year : String) : Long = {
    var result : Long = Long.MinValue;
    val game : (Long, String, LocalDateTime, String, String, String, String, Long, Long, List[String], List[String]) = getGame(game_id, year);
    if (game != null)
      result = game._8;
    return result;
  }

  def updateArticleCount(game_id : Long, year : String, newArticleCount : Long) : Long = {
    var result : Long = Long.MinValue;
    val game : (Long, String, LocalDateTime, String, String, String, String, Long, Long, List[String], List[String]) = getGame(game_id, year);
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
          newArticleCount,
          game._9,
          game._10,
          game._11
        );
      }
    }
    return result;
  }

  def getPreviousGameReviewCount(game_id : Long, year : String) : Long = {
    var result : Long = Long.MinValue;
    val game : (Long, String, LocalDateTime, String, String, String, String, Long, Long, List[String], List[String]) = getGame(game_id, year);
    if (game != null)
      result = game._9
    return result;
  }

  def updateReviewCount(game_id : Long, year : String, newReviewCount : Long) : Long = {
    var result : Long = Long.MinValue;
    val game : (Long, String, LocalDateTime, String, String, String, String, Long, Long, List[String], List[String]) = getGame(game_id, year);
    if (game != null) {
      result = game._9
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
          newReviewCount,
          game._10,
          game._11
        );
      }
    }
    return result;
  }

  def deleteGame(game_id : Long, year : String) : (Long, String, LocalDateTime, String, String, String, String, Long, Long, List[String], List[String]) = {
    val result : (Long, String, LocalDateTime, String, String, String, String, Long, Long, List[String], List[String]) = getGame(game_id, year);
    if (result != null) {
      val spark: SparkSession = connect();
      createGamesCopy("p1.gamesTemp");
      executeDML(spark, s"insert into p1.gamesTemp select * from p1.games where game_id != ${game_id}L");
      executeDML(spark, "drop table p1.games");
      executeDML(spark, "alter table p1.gamesTemp rename to games");
    }
    return result;
  }

  def deleteGames(games : Map[Long, String]) : List[(Long, String, LocalDateTime, String, String, String, String, Long, Long, List[String], List[String])] = {
    val result : ArrayBuffer[(Long, String, LocalDateTime, String, String, String, String, Long, Long, List[String], List[String])] = ArrayBuffer();
    for (game_id : Long <- games.keys) {
      val game : (Long, String, LocalDateTime, String, String, String, String, Long, Long, List[String], List[String]) = deleteGame(game_id, games(game_id));
      if (game != null)
        result += game;
    }
    return result.toList;
  }

  def addReview(review_id : Long, authors : String, title : String, deck : String, lede : String, body : String, publish_date : LocalDateTime, update_date : LocalDateTime, score : Double, review_type : String, game_id : Long) : Unit = {
    val publishDate : String = publish_date.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    val updateDate : String = update_date.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    executeDML(connect(),
      s"insert into p1.reviews partition(game_id=$game_id, year='${publish_date.getYear}') select " +
        s"${review_id}L, " +
        s"'$title', " +
        s"'$deck', " +
        s"'$lede', " +
        s"'$body', " +
        s"to_timestamp('$publishDate'), " +
        s"to_timestamp('$updateDate'), " +
        s"$score, " +
        s"'$review_type'"
    );
    for (author : String <- authors.split("; "))
      if (!authorExists(author))
        addReviewAuthor(review_id, addAuthor(author));
      else
        addReviewAuthor(review_id, getAuthorId(author));
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

  def reviewExists(review_id : Long, year : String) : Boolean = {
    val df : DataFrame = executeQuery(connect(), s"select * from p1.reviews where review_id = ${review_id}L and year = '$year' limit 1");
    return !df.isEmpty;
  }

  def getReview(review_id : Long, year : String) : (Long, String, String, String, String, String, LocalDateTime, LocalDateTime, Double, String, Long) = {
    var result : (Long, String, String, String, String, String, LocalDateTime, LocalDateTime, Double, String, Long) = null;
    val df : DataFrame = executeQuery(connect(), s"select review_id, title, deck, lede, body, publish_date, update_date, score, review_type, game_id from p1.reviews where review_id = ${review_id}L and year = '$year' limit 1");
    if (!df.isEmpty)
      if (!df.take(1)(0).isNullAt(0)) {
        val row : Row = df.take(1)(0);
        val tuple : (Long, String, String, String, String, String, LocalDateTime, LocalDateTime, Double, String, Long) = (
          row.getLong(0),
          getReviewAuthors(review_id),
          row.getString(1),
          row.getString(2),
          row.getString(3),
          row.getString(4),
          row.getTimestamp(5).toLocalDateTime,
          row.getTimestamp(6).toLocalDateTime,
          row.getDouble(7),
          row.getString(8),
          row.getLong(9)
        );
        result = tuple;
      }

    return result;
  }

  def getGameReviews(game_id : Long) : List[(Long, String, String, String, String, String, LocalDateTime, LocalDateTime, Double, String, Long)] = {
    var result : ArrayBuffer[(Long, String, String, String, String, String, LocalDateTime, LocalDateTime, Double, String, Long)] = ArrayBuffer();
    val df : DataFrame = executeQuery(connect(), s"select review_id, title, deck, lede, body, publish_date, update_date, score, review_type, game_id from p1.reviews where game_id = ${game_id}L");
    if (!df.isEmpty)
      if (!df.take(1)(0).isNullAt(0))
        for (row : Row <- df.collect()) {
          val tuple : (Long, String, String, String, String, String, LocalDateTime, LocalDateTime, Double, String, Long) = (
            row.getLong(0),
            getReviewAuthors(row.getLong(0)),
            row.getString(1),
            row.getString(2),
            row.getString(3),
            row.getString(4),
            row.getTimestamp(5).toLocalDateTime,
            row.getTimestamp(6).toLocalDateTime,
            row.getDouble(7),
            row.getString(8),
            row.getLong(9)
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
    for (review : (Long, String, String, String, String, String, LocalDateTime, LocalDateTime, Double, String, Long) <- result)
      deleteReviewAuthors(review._1);

    return result;
  }

  def addArticle(article_id : Long, authors : String, title : String, deck : String, lede : String, body : String, publish_date : LocalDateTime, update_date : LocalDateTime, categories : Map[Long, String], game_id : Long) : Unit = {
    val publishDate : String = publish_date.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    val updateDate : String = update_date.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

    executeDML(connect(),
      s"insert into p1.articles partition(game_id=$game_id, year='${publish_date.getYear}') select " +
        s"${article_id}L, " +
        s"'$title', " +
        s"'$deck', " +
        s"'$lede', " +
        s"'$body', " +
        s"to_timestamp('$publishDate'), " +
        s"to_timestamp('$updateDate'), "
    );
    authors.split("; ").foreach(a => {
      if (!authorExists(a))
        addArticleAuthor(article_id, addAuthor(a));
      else
        addArticleAuthor(article_id, getAuthorId(a));
    });
    for (category : (Long, String) <- categories) {
      if (!categoryExists(category._1))
        addCategory(category._1, category._2);
      addArticleCategory(article_id, category._1);
    };
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

  def articleExists(article_id : Long, year : String) : Boolean = {
    val df : DataFrame = executeQuery(connect(), s"select * from p1.articles where article_id = ${article_id}L and year = '$year' limit 1");
    return !df.isEmpty;
  }

  def getArticle(article_id : Long, year : String) : (Long, String, String, String, String, String, LocalDateTime, LocalDateTime, Map[Long, String], Long) = {
    var result : (Long, String, String, String, String, String, LocalDateTime, LocalDateTime, Map[Long, String], Long) = null;
    val df : DataFrame = executeQuery(connect(), s"select article_id, title, deck, lede, body, publish_date, update_date, game_id from p1.articles where article_id = ${article_id}L and year = '$year' limit 1");
    if (!df.isEmpty)
      if (!df.take(1)(0).isNullAt(0)) {
        val row : Row = df.take(1)(0);
        val categories : Map[Long, String] = row.get(8).asInstanceOf[Map[Long, String]];
        val tuple : (Long, String, String, String, String, String, LocalDateTime, LocalDateTime, Map[Long, String], Long) = (
          row.getLong(0),
          getArticleAuthors(row.getLong(0)),
          row.getString(1),
          row.getString(2),
          row.getString(3),
          row.getString(4),
          row.getTimestamp(5).toLocalDateTime,
          row.getTimestamp(6).toLocalDateTime,
          getArticleCategories(row.getLong(0)),
          row.getLong(7)
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
    val df : DataFrame = executeQuery(connect(), s"select article_id, title, deck, lede, body, publish_date, update_date, game_id from p1.articles where game_id = ${game_id}L");
    if (!df.isEmpty)
      if (!df.take(1)(0).isNullAt(0))
        for (row : Row <- df.collect()) {
          val tuple : (Long, String, String, String, String, String, LocalDateTime, LocalDateTime, Map[Long, String], Long) = (
            row.getLong(0),
            getArticleAuthors(row.getLong(0)),
            row.getString(1),
            row.getString(2),
            row.getString(3),
            row.getString(4),
            row.getTimestamp(5).toLocalDateTime,
            row.getTimestamp(6).toLocalDateTime,
            getArticleCategories(row.getLong(0)),
            row.getLong(7)
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
    for (article : (Long, String, String, String, String, String, LocalDateTime, LocalDateTime, Map[Long, String], Long) <- result) {
      deleteArticleAuthors(article._1);
      deleteArticleCategories(article._1);
    }

    return result;
  }

  protected def addAuthor(author : String) : Long = {
    val result : Long = getNextAuthorId();
    executeDML(connect(), s"insert into p1.authors values ($result, '$author')");
    return result;
  }

  protected def authorExists(author : String) : Boolean = {
    val df : DataFrame = executeQuery(connect(), s"select * from p1.authors where author = '$author' limit 1");
    return !df.isEmpty;
  }

  protected def getAuthorId(author : String) : Long = {
    var result : Long = 0L;
    val df : DataFrame = executeQuery(connect(), s"select author_id from p1.authors where author = '$author' limit 1");
    if (!df.isEmpty)
      result = df.take(1)(0).getLong(0);
    return result;
  }

  protected def getNextAuthorId() : Long = {
    val df : DataFrame = executeQuery(connect(), "select max(author_id) from p1.authors");
    if (!df.isEmpty)
      if (!df.take(1)(0).isNullAt(0))
        return df.take(1)(0).getLong(0) + 1;
    return 1L;
  }

  protected def addArticleAuthor(article_id : Long, author_id : Long) : Unit = {
    executeDML(connect(), s"insert into p1.articleAuthors values ($article_id, $author_id)");
  }

  protected def getArticleAuthors(article_id : Long) : String = {
    var result : String = "";
    val df : DataFrame = executeQuery(connect(), s"select au.author from p1.authors au, p1.articleAuthors aa, p1.articles a where a.article_id = $article_id and a.article_id = aa.article_id and aa.author_id = au.author_id order by au.author");
    if (!df.isEmpty) {
      var authors : ArrayBuffer[String] = ArrayBuffer();
      for (row : Row <- df.collect())
        authors += row.getString(0);
      result = authors.reduce((x, y) => x + "; " + y);
    }
    return result;
  }

  protected def deleteArticleAuthors(article_id : Long) : String = {
    val result : String = getArticleAuthors(article_id);
    if (result.nonEmpty) {
      val spark : SparkSession = connect();
      createArticleAuthorsCopy("p1.articleAuthorsTemp");
      executeDML(spark, s"insert into p1.articleAuthorsTemp select * from p1.articleAuthors where article_id != $article_id");
      executeDML(spark, "drop table p1.articleAuthors");
      executeDML(spark, "alter table p1.articleAuthorsTemp rename to articleAuthors");
    }

    return result;
  }

  protected def addReviewAuthor(review_id : Long, author_id : Long) : Unit = {
    executeDML(connect(), s"insert into p1.reviewAuthors values ($review_id, $author_id)");
  }

  protected def getReviewAuthors(review_id : Long) : String = {
    var result : String = "";
    val df : DataFrame = executeQuery(connect(), s"select a.author from p1.authors a, p1.reviewAuthors ra, p1.reviews r where r.review_id = $review_id and r.review_id = ra.review_id and ra.author_id = a.author_id order by a.author");
    if (!df.isEmpty) {
      var authors : ArrayBuffer[String] = ArrayBuffer();
      for (row : Row <- df.collect())
        authors += row.getString(0);
      result = authors.reduce((x, y) => x + "; " + y);
    }
    return result;
  }

  protected def deleteReviewAuthors(review_id : Long) : String = {
    val result : String = getReviewAuthors(review_id);
    if (result.nonEmpty) {
      val spark : SparkSession = connect();
      createReviewAuthorsCopy("p1.reviewAuthorsTemp");
      executeDML(spark, s"insert into p1.reviewAuthorsTemp select * from p1.reviewAuthors where review_id != $review_id");
      executeDML(spark, "drop table p1.reviewAuthors");
      executeDML(spark, "alter table p1.reviewAuthorsTemp rename to reviewAuthors");
    }

    return result;
  }

  protected def addCategory(category_id : Long, category : String) : Unit = {
    executeDML(connect(), s"insert into p1.categories values ($category_id, '$category')");
  }

  protected def categoryExists(category_id : Long) : Boolean = {
    val df : DataFrame = executeQuery(connect(), s"select * from p1.categories where category_id = $category_id");
    return !df.isEmpty;
  }

  protected def addArticleCategory(article_id : Long, category_id : Long) : Unit = {
    executeDML(connect(), s"insert into p1.articleCategory values ($article_id, $category_id)");
  }

  protected def getArticleCategories(article_id : Long) : Map[Long, String] = {
    var result : Map[Long, String] = Map();
    val df : DataFrame = executeQuery(connect(), s"select c.category_id, c.category from p1.categories c, p1.articleCategories ac, p1.articles a where a.article_id = $article_id and a.article_id = ac.article_id and ac.category_id = c.category_id order by c.category_id");
    if (!df.isEmpty)
      for(row : Row <- df.collect())
        result += (row.getLong(0) -> row.getString(1));
    return result;
  }

  protected def deleteArticleCategories(article_id : Long) : Map[Long, String] = {
    val result : Map[Long, String] = getArticleCategories(article_id);
    if (result.nonEmpty) {
      val spark : SparkSession = connect();
      createArticleCategoriesCopy("p1.articleCategoriesTemp");
      executeDML(spark, s"insert into p1.articleCategoriesTemp select * from p1.articleCategories where article_id != $article_id");
      executeDML(spark, "drop table p1.articleCategories");
      executeDML(spark, "alter table p1.articleCategoriesTemp rename to articleCategories");
    }

    return result;
  }

  def addGenre(genre : String) : Long = {
    val result : Long = getNextGenreId();
    executeDML(connect(), s"insert into p1.genres values ($result, '$genre')");
    return result;
  }

  def genreExists(genre : String) : Boolean = {
    val df : DataFrame = executeQuery(connect(), s"select * from p1.genres where genre = '$genre' limit 1");
    return !df.isEmpty;
  }

  def getGenreId(genre : String) : Long = {
    val df : DataFrame = executeQuery(connect(), s"select genre_id from p1.genres where genre = '$genre' limit 1");
    if (!df.isEmpty)
      return df.take(1)(0).getLong(0);
    return 0L;
  }

  protected def getNextGenreId() : Long = {
    val df : DataFrame = executeQuery(connect(), s"select max(genre_id) from p1.genres");
    if (!df.isEmpty) {
      if (!df.take(1)(0).isNullAt(0))
        return df.take(1)(0).getLong(0) + 1;
    };
    return 1L;
  }

  protected def addGameGenre(game_id : Long, genre_id : Long) : Unit = {
    executeDML(connect(), s"insert into p1.gameGenres values ($game_id, $genre_id)");
  }

  protected def getGameGenres(game_id : Long) : List[String] = {
    var result : ArrayBuffer[String] = ArrayBuffer();
    val df : DataFrame = executeQuery(connect(), s"select ge.genre from p1.genres ge, p1.gameGenres gg, p1.games g where g.game_id = $game_id and g.game_id = gg.game_id and gg.genre_id = ge.genre_id order by ge.genre_id");
    if (!df.isEmpty)
      for(row : Row <- df.collect())
        result += row.getString(0);
    return result.toList;;
  }

  protected def deleteGameGenres(game_id : Long) : List[String] = {
    val result : List[String] = getGameGenres(game_id);
    if (result.nonEmpty) {
      val spark : SparkSession = connect();
      createGameGenresCopy("p1.gameGenresTemp");
      executeDML(spark, s"insert into p1.gameGenresTemp select * from p1.gameGenres where game_id != $game_id");
      executeDML(spark, "drop table p1.gameGenres");
      executeDML(spark, "alter table p1.gameGenresTemp rename to gameGenres");
    }
    return result;
  }

  def addTheme(theme : String) : Long = {
    val result : Long = getNextThemeId();
    executeDML(connect(), s"insert into p1.themes values ($result, '$theme')");
    return result;
  }

  def themeExists(theme : String) : Boolean = {
    val df : DataFrame = executeQuery(connect(), s"select * from p1.themes where theme = '$theme' limit 1");
    return !df.isEmpty;
  }

  def getThemeId(theme : String) : Long = {
    val df : DataFrame = executeQuery(connect(), s"select theme_id from p1.themes where theme = '$theme' limit 1");
    if (!df.isEmpty)
      return df.take(1)(0).getLong(0);
    return 0L;
  }

  protected def getNextThemeId() : Long = {
    val df : DataFrame = executeQuery(connect(), s"select max(theme_id) from p1.themes");
    if (!df.isEmpty)
      if (!df.take(1)(0).isNullAt(0))
        return df.take(1)(0).getLong(0) + 1;
    return 1L;
  }

  protected def addGameTheme(game_id : Long, theme_id : Long) : Unit = {
    executeDML(connect(), s"insert into p1.gameThemes values ($game_id, $theme_id)");
  }

  protected def getGameThemes(game_id : Long) : List[String] = {
    var result : ArrayBuffer[String] = ArrayBuffer();
    val df : DataFrame = executeQuery(connect(), s"select t.theme from p1.themes t, p1.gameThemes gt, p1.games g where g.game_id = $game_id and g.game_id = gt.game_id and gt.theme_id = t.theme_id order by t.theme_id");
    if (!df.isEmpty)
      for (row : Row <- df.collect())
        result += row.getString(0);
    return result.toList;
  }

  protected def deleteGameThemes(game_id : Long) : List[String] = {
    val result : List[String] = getGameThemes(game_id);
    if (result.nonEmpty) {
      val spark : SparkSession = connect();
      createGameThemesCopy("p1.gameThemesTemp");
      executeDML(spark, s"insert into p1.gameThemesTemp select * from p1.gameThemes where game_id != $game_id");
      executeDML(spark, "drop table p1.gameThemes");
      executeDML(spark, "alter table p1.gameThemesTemp rename to gameThemes");
    }
    return result;
  }
}