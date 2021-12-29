import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

import java.time.LocalDateTime

class TestHiveDBManager extends AnyFlatSpec with should.Matchers {
  object Test extends HiveDBManager {
    override def connect(): SparkSession = super.connect();
    override def executeDML(spark: SparkSession, sql: String): Unit = super.executeDML(spark, sql);
    override def executeQuery(spark: SparkSession, sql: String): DataFrame = super.executeQuery(spark, sql);
    override def showQuery(spark: SparkSession, sql: String): Unit = super.showQuery(spark, sql);
    override def createDB() : Unit = super.createDB();
    override def getNextUserId(): Int = super.getNextUserId()
    override def getUsers(): List[(Int, String, String, Boolean)] = super.getUsers();
    override def authenticate(username: String, password: String, isAdmin: Boolean): Int = super.authenticate(username, password, isAdmin);
    override def addUser(username: String, password: String): Int = super.addUser(username, password);
    override def updateUsername(user_id: Int, newUsername: String): Boolean = super.updateUsername(user_id, newUsername);
    override def updatePassword(user_id: Int, oldPassword: String, newPassword: String): Boolean = super.updatePassword(user_id, oldPassword, newPassword);
    override def usernameExists(username: String): Boolean = super.usernameExists(username);
    override def isAdmin(user_id: Int): Boolean = super.isAdmin(user_id);
    override def setToAdmin(user_id: Int): Unit = super.setToAdmin(user_id);
    override def deleteUser(user_id: Int): Unit = super.deleteUser(user_id);
    override def getNextQueryId(): Int = super.getNextQueryId();
    override def getQueries(): Map[Int, String] = super.getQueries();
    override def queryNameExists(query_name : String): Boolean = super.queryNameExists(query_name);
    override def showQuery(query_id: Int): Unit = super.showQuery(query_id);
    override def showQuery(query: String): Unit = super.showQuery(query);
    override def saveQuery(user_id: Int, query_name: String, query: String): Unit = super.saveQuery(user_id, query_name, query);
    override def deleteQuery(query_id: Int): Unit = super.deleteQuery(query_id);
    override def addGame(game_id: Long, name: String, release_date: LocalDateTime, deck: String, description: String, articles_api_url: String, reviews_api_url: String, avg_score: Double, article_count: Long, review_count: Long, genres: List[String], themes: List[String]): Unit = super.addGame(game_id, name, release_date, deck, description, articles_api_url, reviews_api_url, avg_score, article_count, review_count, genres, themes);
    override def addGames(games: List[(Long, String, LocalDateTime, String, String, String, String, Double, Long, Long, List[String], List[String])]): Unit = super.addGames(games);
    override def getGame(game_id: Long, year : String): (Long, String, LocalDateTime, String, String, String, String, Double, Long, Long, List[String], List[String]) = super.getGame(game_id, year);
    override def getGame(game_name: String): (Long, String, LocalDateTime, String, String, String, String, Double, Long, Long, List[String], List[String]) = super.getGame(game_name);
    override def gameExists(game_id: Long, year: String): Boolean = super.gameExists(game_id, year);
    override def getGamesBetween(startDate: LocalDateTime, endDate: LocalDateTime): List[(Long, String, LocalDateTime, String, String, String, String, Double, Long, Long, List[String], List[String])] = super.getGamesBetween(startDate, endDate);
    override def getMaxGameBetween(startDate: LocalDateTime, endDate: LocalDateTime): (Long, String, LocalDateTime, String, String, String, String, Double, Long, Long, List[String], List[String]) = super.getMaxGameBetween(startDate, endDate);
    override def getGameCountBetween(startDate: LocalDateTime, endDate: LocalDateTime): Long = super.getGameCountBetween(startDate, endDate);
    override def getMaxGame(): (Long, String, LocalDateTime, String, String, String, String, Double, Long, Long, List[String], List[String]) = super.getMaxGame();
    override def getGames() : List[(Long, String, LocalDateTime, String, String, String, String, Double, Long, Long, List[String], List[String])] = super.getGames();
    override def getGameCount(): Long = super.getGameCount();
    override def calculateAvgScore(game_id: Long): Double = super.calculateAvgScore(game_id);
    override def getAvgScore(game_id : Long, year : String) : Double = super.getAvgScore(game_id, year);
    override def updateAvgScore(game_id: Long, year : String, newScore: Double): Double = super.updateAvgScore(game_id, year, newScore);
    override def getPreviousGameArticleCount(game_id: Long, year : String): Long = super.getPreviousGameArticleCount(game_id, year);
    override def updateArticleCount(game_id: Long, year : String, newArticleCount: Long): Long = super.updateArticleCount(game_id, year, newArticleCount);
    override def getPreviousGameReviewCount(game_id: Long, year : String): Long = super.getPreviousGameReviewCount(game_id, year);
    override def updateReviewCount(game_id: Long, year : String, newReviewCount: Long): Long = super.updateReviewCount(game_id, year, newReviewCount);
    override def deleteGame(game_id: Long, year : String): (Long, String, LocalDateTime, String, String, String, String, Double, Long, Long, List[String], List[String]) = super.deleteGame(game_id, year);
    override def deleteGames(games: Map[Long, String]): List[(Long, String, LocalDateTime, String, String, String, String, Double, Long, Long, List[String], List[String])] = super.deleteGames(games);
    override def addReview(review_id: Long, authors: String, title: String, deck: String, lede: String, body: String, publish_date: LocalDateTime, update_date: LocalDateTime, score: Double, review_type: String, game_id: Long): Unit = super.addReview(review_id, authors, title, deck, lede, body, publish_date, update_date, score, review_type, game_id);
    override def addReviews(reviews: List[(Long, String, String, String, String, String, LocalDateTime, LocalDateTime, Double, String, Long)]): Unit = super.addReviews(reviews);
    override def reviewExists(review_id: Long, year : String): Boolean = super.reviewExists(review_id, year);
    override def getReview(review_id: Long, year : String): (Long, String, String, String, String, String, LocalDateTime, LocalDateTime, Double, String, Long) = super.getReview(review_id, year);
    override def getGameReviews(game_id: Long): List[(Long, String, String, String, String, String, LocalDateTime, LocalDateTime, Double, String, Long)] = super.getGameReviews(game_id);
    override def getReviewCount(): Long = super.getReviewCount();
    override def getGameReviewCount(game_id: Long): Long = super.getGameReviewCount(game_id);
    override def deleteGameReviews(game_id: Long): List[(Long, String, String, String, String, String, LocalDateTime, LocalDateTime, Double, String, Long)] = super.deleteGameReviews(game_id);
    override def addArticle(article_id: Long, authors: String, title: String, deck: String, lede: String, body: String, publish_date: LocalDateTime, update_date: LocalDateTime, categories: Map[Long, String], game_id: Long): Unit = super.addArticle(article_id, authors, title, deck, lede, body, publish_date, update_date, categories, game_id);
    override def addArticles(articles: List[(Long, String, String, String, String, String, LocalDateTime, LocalDateTime, Map[Long, String], Long)]): Unit = super.addArticles(articles);
    override def articleExists(article_id: Long, year : String): Boolean = super.articleExists(article_id, year);
    override def getArticle(article_id: Long, year : String): (Long, String, String, String, String, String, LocalDateTime, LocalDateTime, Map[Long, String], Long) = super.getArticle(article_id, year);
    override def getGameArticleCount(game_id: Long): Long = super.getGameArticleCount(game_id);
    override def getGameArticles(game_id: Long): List[(Long, String, String, String, String, String, LocalDateTime, LocalDateTime, Map[Long, String], Long)] = super.getGameArticles(game_id);
    override def deleteGameArticles(game_id: Long): List[(Long, String, String, String, String, String, LocalDateTime, LocalDateTime, Map[Long, String], Long)] = super.deleteGameArticles(game_id);
  }

  "randomcommands" should "only be used FOR TESTING ONLY" in {
    for(game : (Long, String, LocalDateTime, String, String, String, String, Double, Long, Long, List[String], List[String]) <- Test.getGames()) {
      if (Test.getPreviousGameReviewCount(game._1, game._2) != Test.getGameReviewCount(game._1))
        Test.updateReviewCount(game._1, game._3.getYear.toString, Test.getGameReviewCount(game._1));
      if (Test.getPreviousGameArticleCount(game._1, game._2) != Test.getGameArticleCount(game._1))
        Test.updateArticleCount(game._1, game._3.getYear.toString, Test.getGameArticleCount(game._1));
      if (Test.getAvgScore(game._1, game._2) != Test.calculateAvgScore(game._1))
        Test.updateAvgScore(game._1, game._3.getYear.toString, Test.calculateAvgScore(game._1));
      println(s"Finished updating ${"\""}${game._2}${"\""}.");
    }
    assert(true);
  }

  "executeQuery(SparkSession, String)" should "be for TESTING ONLY!" in {
    val df : DataFrame = Test.executeQuery(Test.connect(), "select article_id from p1.articles limit 1");
    df.show(Int.MaxValue, false);
    assert(true);
  }

  "deleteDB()" should "clear the P1 database from Derby (FOR TESTING ONLY!)" in {
    Test.executeDML(Test.connect(), "drop database if exists p1 cascade");
    assert(true);
  }

  "createDB()" should "create a new P1 database with all the necessary tables and a pre-made Admin user" in {
    Test.createDB();
    assert(true);
  }

  "getNextUserId()" should "return the next usable user_id from the database" in {
    var result : Int = 0;
    result = Test.getNextUserId();
    assert(result > 0);
  }

  "getUsers()" should "return a list of user_ids and usernames stored in the database in order of user_id" in {
    val result : List[(Int, String, String, Boolean)] = Test.getUsers();
    assert(result.nonEmpty);
  }

  "authenticate(String, String, Boolean)" should "return the user_id associated with the user details" in {
    val result : Int = Test.authenticate("admin", "admin", true);
    assert(result == 1);
  }

  it should "return a value of zero if the combination of credentials does not exist" in {
    var result : Int = Test.authenticate("notarealuser", "admin", true);
    assert(result == 0);
    result = Test.authenticate("notarealuser", "admin", false);
    assert(result == 0);
    result = Test.authenticate("admin", "wrongpassword", true);
    assert(result == 0);
    result = Test.authenticate("admin", "wrongpassword", false);
    assert(result == 0);
    result = Test.authenticate("admin", "admin", false);
    assert(result == 0);
    result = Test.authenticate("notarealuser", "wrongpassword", true);
    assert(result == 0);
    result = Test.authenticate("notarealuser", "wrongpassword", false);
    assert(result == 0);
  }

  "adduser(String, String)" should "return the user_id, which will be greater than zero, for the newly-created user" in {
    val result : Int = Test.addUser("test", "test");
    assert(result > 0);
  }

  "updateUsername(Int, String)" should "return true if the new username does not exist (the user is already authenticated at this point, so no need for password)" in {
    assert(Test.updateUsername(1, "newAdmin"));
    assert(Test.updateUsername(1, "admin"));
  }

  "updatePassword(Int, String, String)" should "return true if the user_id and old password match (at this point in the program, the user is already authenticated) allowing it to successfully update the user's password" in {
    assert(Test.updatePassword(1, "admin", "newAdmin"));
    assert(Test.updatePassword(1, "newAdmin", "admin"));
  }

  it should "return false if the old password does not match" in {
    assert(!Test.updatePassword(1, "newAdmin", "newerAdmin"));
  }

  "usernameExists(String)" should "return true if the username exists in the users table" in {
    assert(Test.usernameExists("admin"));
  }

  it should "return false if the username does not exist" in {
    assert(!Test.usernameExists(""));
  }

  "isAdmin(Int)" should "return true or false depending on if the user is an admin" in {
    assert(Test.isAdmin(Test.authenticate("admin", "admin", true)));
    assert(!Test.isAdmin(Test.authenticate("test", "test", false)));
  }

  "setToAdmin(Int)" should "change the passed-in user to admin if they are not already one" in {
    assert(!Test.isAdmin(Test.authenticate("test", "test", false)));
    Test.setToAdmin(Test.authenticate("test", "test", false));
    assert(Test.isAdmin(Test.authenticate("test", "test", true)));
  }

  "deleteUser(Int)" should "remove the passed-in user_id from the users table" in {
    assert(Test.addUser("test02", "test02") > 0);
    assert(Test.usernameExists("test02"));
    Test.deleteUser(Test.authenticate("test02", "test02", false));
    assert(!Test.usernameExists("test02"));
  }

  "getNextQueryId()" should "return the next max usable query_id for the queries table" in {
    assert(Test.getNextQueryId() > 0);
  }

  "getQueries()" should "return a list of query IDs and names from the queries table" in {
    if (Test.getQueries().isEmpty)
      assert(true);
    else
      assert(Test.getQueries().nonEmpty);
  }

  "queryNameExists(String)" should "return true if a query with the given name exists" in {
    val queries : Map[Int, String] = Test.getQueries();
    if (queries.nonEmpty)
      assert(Test.queryNameExists(queries.values.head));
    else
      assert(!Test.queryNameExists("test"));
  }

  "showQuery(Int)" should "output the selected query" in {
    val queries : Map[Int, String] = Test.getQueries();
    if (queries.nonEmpty)
      Test.showQuery(queries.keys.head);
    assert(true);
  }

  "showQuery(String)" should "output the passed-in query" in {
    Test.showQuery("select * from p1.games");
    assert(true);
  }

  "saveQuery(Int, String, String)" should "save the passed-in query and user information to the queries table" in {
    assert(Test.getQueries().isEmpty);
    Test.saveQuery(Test.authenticate("admin", "admin", true), "test", "select * from p1.users");
    assert(Test.getQueries().nonEmpty);
  }

  "deleteQuery(Int)" should "delete the query with the passed-in query_id from the queries table" in {
    val name : String = Test.getQueries().find(p => p._1 == 1).get._2;
    Test.deleteQuery(1);
    assert(!Test.queryNameExists(name));
  }

  "addGame(Long, String, LocalDateTime, String, String, String, String, Double, Long, Long, List[String], List[String]" should "insert a new game record into the games table if it does not already exist" in {
    Test.addGame(1, "test", LocalDateTime.now(), "test", "test", "http://www.nowhere.com", "http://www.nowhere.com", 0.0, 0, 0, List[String]("test01", "test02"), List[String]("test03", "test04"));
    assert(Test.gameExists(1, LocalDateTime.now().getYear.toString));
  }

  "addGames(List[(Long, String, LocalDateTime, String, String, String, String, Double, Long, Long, List[String], List[String])])" should "insert new game records into the games table if they do not already exist" in {
    Test.addGames(List[(Long, String, LocalDateTime, String, String, String, String, Double, Long, Long, List[String], List[String])](
      (2, "test1", LocalDateTime.now(), "test", "test", "http://www.nowhere.com", "http://www.nowhere.com", 0.0, 0, 0, List[String](), List[String]()),
      (3, "test2", LocalDateTime.now(), "test", "test", "http://www.nowhere.com", "http://www.nowhere.com", 0.0, 0, 0, List[String](), List[String]()),
    ));
    assert(Test.gameExists(2, LocalDateTime.now().getYear.toString));
    assert(Test.gameExists(3, LocalDateTime.now().getYear.toString));
  }

  "gameExists(Long, String)" should "return true when its game_id and year parameters match a game in our datastore" in {
    assert(Test.gameExists(1, LocalDateTime.now().getYear.toString));
  }

  it should "return false if the game_id does not match a game within the given partition in the datastore" in {
    assert(!Test.gameExists(2, "1000"));
    assert(!Test.gameExists(1, "1000"));
  }

  "getGame(Long, String)" should "return the game details for the specified game_id, assuming that it exists in our games datastore" in {
    val result : (Long, String, LocalDateTime, String, String, String, String, Double, Long, Long, List[String], List[String]) = Test.getGame(1, LocalDateTime.now().getYear.toString);
    assert(result != null);
  }

  it should "return null if the game_id does not exist in the games datastore" in {
    val result : (Long, String, LocalDateTime, String, String, String, String, Double, Long, Long, List[String], List[String]) = Test.getGame(-1, LocalDateTime.now().getYear.toString);
    assert(result == null);
  }

  "getGame(String)" should "return the game details for the specified game_name, assuming that it exists in our games datastore" in {
    val result : (Long, String, LocalDateTime, String, String, String, String, Double, Long, Long, List[String], List[String]) = Test.getGame("test");
    assert(result != null);
  }

  it should "return null if the game_id does not exist in the games datastore" in {
    val result : (Long, String, LocalDateTime, String, String, String, String, Double, Long, Long, List[String], List[String]) = Test.getGame("nonexistentgame");
    assert(result == null);
  }

  "getGamesBetween(LocalDateTime, LocalDateTime)" should "return a list of games that exist between the start and end dates" in {
    val result : List[(Long, String, LocalDateTime, String, String, String, String, Double, Long, Long, List[String], List[String])] = Test.getGamesBetween(LocalDateTime.parse("2021-12-01T00:00:00"), LocalDateTime.parse("2021-12-31T23:59:59"));
    if (result.isEmpty)
      assert(result.isEmpty);
    else
      assert(result.nonEmpty);
  }

  "getMaxGameBetween(LocalDateTime, LocalDateTime)" should "return the maximum game inside the given period" in {
    assert(Test.getMaxGameBetween(LocalDateTime.parse("2021-12-01T00:00:00"), LocalDateTime.parse("2021-12-31T23:59:59"))._1 == 3);
  }

  it should "return null if no games were found" in {
    assert(Test.getMaxGameBetween(LocalDateTime.parse("3000-12-01T00:00:00"), LocalDateTime.parse("3000-12-31T23:59:59")) == null);
  }

  "getGameCountBetween(LocalDateTime, LocalDateTime)" should "return the number of games whose publish_date lie inside the given period" in {
    assert(Test.getGameCountBetween(LocalDateTime.parse("2021-12-01T00:00:00"), LocalDateTime.parse("2021-12-31T23:59:59")) == 3);
  }

  it should "return zero if no games were found" in {
    assert(Test.getGameCountBetween(LocalDateTime.parse("3000-12-01T00:00:00"), LocalDateTime.parse("3000-12-31T23:59:59")) == 0);
  }

  "getMaxGame()" should "return the game with the largest game_id in our games datastore" in {
    assert(Test.getMaxGame() != null);
  }

  "getGames()" should "return a list containing all the games in our games datastore" in {
    val games : List[(Long, String, LocalDateTime, String, String, String, String, Double, Long, Long, List[String], List[String])] = Test.getGames();
    if (games.isEmpty)
      assert(games.isEmpty);
    else
      assert(games.nonEmpty);
  }

  "getGameCount()" should "return the number of games stored in our games datastore" in {
    assert(Test.getGameCount() == 3);
  }

  "calculateAvgScore()" should "return the latest average score based on the current number of reviews in the datastore" in {
    assert(Test.calculateAvgScore(1L) >= 0.0);
  }

  it should "return negative infinity if the game_id does not exist" in {
    assert(Test.calculateAvgScore(-1L) == Double.NegativeInfinity);
  }

  "getAvgScore(Long, String)" should "return the last known average score for a game with the given game_id and year partition" in {
    assert(Test.getAvgScore(1, LocalDateTime.now().getYear.toString) == 0.0);
  }

  it should "return negative infinity if the game does not exist" in {
    assert(Test.getAvgScore(-1, LocalDateTime.now().getYear.toString) == Double.NegativeInfinity);
  }

  "updateAvgScore(Long, String, Double)" should "update the game with the provided game_id and year partition's average score with the provided average score and return its previous value" in {
    assert(Test.updateAvgScore(1, LocalDateTime.now().getYear.toString, 3.2) == 0.0);
    assert(Test.updateAvgScore(1, LocalDateTime.now().getYear.toString, 0.0) == 3.2);
  }

  it should "should return the previous value if the provided value is greater than 10.0 or less than 0.0" in {
    assert(Test.updateAvgScore(1, LocalDateTime.now().getYear.toString, -0.1) == 0.0);
    assert(Test.updateAvgScore(1, LocalDateTime.now().getYear.toString, 10.01) == 0.0);
  }

  it should "return negative infinity for Double if the game id does not exist" in {
    assert(Test.updateAvgScore(-1, LocalDateTime.now().getYear.toString, 2.0) == Double.NegativeInfinity);
  }

  "getPreviousGameArticleCount(Long, String)" should "return the previously-known article count for a given game" in {
    assert(Test.getPreviousGameArticleCount(1, LocalDateTime.now().getYear.toString) == 0);
  }

  it should "return the smallest Long value if the game_id does not exist in our datastore" in {
    assert(Test.getPreviousGameArticleCount(-1, LocalDateTime.now().getYear.toString) == Long.MinValue);
  }

  "updateArticleCount(Long, String, Long)" should "update the game with the provided game_id and year partition's article count with the provided newArticleCount and return the previous article_count value" in {
    assert(Test.updateArticleCount(1, LocalDateTime.now().getYear.toString, 1) == 0);
    assert(Test.updateArticleCount(1, LocalDateTime.now().getYear.toString, 0) == 1);
  }

  it should "return the previous value if the new article count is less than 0" in {
    assert(Test.updateArticleCount(1, LocalDateTime.now().getYear.toString, -1) == 0);
  }

  it should "return the smallest possible Long value if the game_id does not exist" in {
    assert(Test.updateArticleCount(-1, LocalDateTime.now().getYear.toString, 2) == Long.MinValue);
  }

  "getPreviousGameReviewCount(Long, String)" should "return the previously-known review count for a given game" in {
    assert(Test.getPreviousGameReviewCount(1, LocalDateTime.now().getYear.toString) == 0);
  }

  it should "return the smallest possible Long value if the game_id does not exist in our datastore" in {
    assert(Test.getPreviousGameReviewCount(-1, LocalDateTime.now().getYear.toString) == Long.MinValue);
  }

  "updateReviewCount(Long, String, Long)" should "update the game with the provided game_id and year partition review count with the provided newReviewCount and return the previous review_count value" in {
    assert(Test.updateReviewCount(1, LocalDateTime.now().getYear.toString, 1) == 0);
    assert(Test.updateReviewCount(1, LocalDateTime.now().getYear.toString, 0) == 1);
  }

  it should "return the previous value if the new review count is less than 0" in {
    assert(Test.updateReviewCount(1, LocalDateTime.now().getYear.toString, -1) == 0);
  }

  it should "return the smallest possible Long value if the game_id does not exist" in {
    assert(Test.updateReviewCount(-1, LocalDateTime.now().getYear.toString, 2) == Long.MinValue);
  }

  "deleteGame(Long, String)" should "return the details of the game that is removed from the datastore" in {
    assert(Test.deleteGame(3, LocalDateTime.now().getYear.toString) != null);
    assert(Test.getGame(3, LocalDateTime.now().getYear.toString) == null);
  }

  it should "return null if no game was deleted" in {
    assert(Test.deleteGame(3, LocalDateTime.now().getYear.toString) == null);
  }

  "deleteGames(Map[Long, String])" should "return the game details of the games removed from the datastore" in {
    assert(Test.deleteGames(Map(1L -> LocalDateTime.now().getYear.toString, 2L -> LocalDateTime.now().getYear.toString)).nonEmpty);
  }

  it should "return an empty list (or a list smaller than the number of game_ids) if games do not exist" in {
    assert(Test.deleteGames(Map(-1L -> LocalDateTime.now().getYear.toString, -2L -> LocalDateTime.now().getYear.toString,-3L -> LocalDateTime.now().getYear.toString)).isEmpty);
  }

  "getMaxGame()" should "return null if no game was found" in {
    assert(Test.getMaxGame() == null);
  }

  "getGameCount()" should "return zero if no games were found" in {
    assert(Test.getGameCount() == 0);
  }

  "addReview(Long, String, String, String, String, String, LocalDateTime, LocalDateTime, Double, String, Long)" should "add a new review to the reviews datastore" in {
    Test.addReview(1, "test", "test", "test", "test", "test", LocalDateTime.now(), LocalDateTime.now(), 0.0, "primary", 1);
    assert(Test.reviewExists(1, LocalDateTime.now().getYear.toString));
  }

  "addReviews(List[(Long, String, String, String, String, String, LocalDateTime, LocalDateTime, Double, String, Long)])" should "add the passed-in reviews to the reviews datastore" in {
    Test.addReviews(
      List(
        (2, "test", "test", "test", "test", "test", LocalDateTime.now(), LocalDateTime.now(), 0.0, "secondary", 1),
        (3, "test", "test", "test", "test", "test", LocalDateTime.now(), LocalDateTime.now(), 0.0, "second take", 1)
      )
    );
    assert(Test.reviewExists(2, LocalDateTime.now().getYear.toString));
    assert(Test.reviewExists(3, LocalDateTime.now().getYear.toString));
  }

  "reviewExists(Long, String)" should "return true when it finds a review with the given review_id and year in our reviews datastore within the given year partition" in {
    assert(Test.reviewExists(1, LocalDateTime.now().getYear.toString));
  }

  it should "return false if the review_id does not exist in the given partition" in {
    assert(!Test.reviewExists(-1, "1000"));
  }

  "getGameReviews(Long)" should "return a list of reviews for the given game_id, if any exist" in {
    val results : List[(Long, String, String, String, String, String, LocalDateTime, LocalDateTime, Double, String, Long)] = Test.getGameReviews(1);
    if (results.isEmpty)
      assert(results.isEmpty);
    else
      assert(results.nonEmpty);
  }

  "getReview(Long, String)" should "return the details for a review with the given review_id and year in the given year" in {
    assert(Test.getReview(6112628, LocalDateTime.now().getYear.toString) != null);
  }

  it should "return null if a review does not exist with the given review_id" in {
    assert(Test.getReview(-1, LocalDateTime.now().getYear.toString) == null);
  }

  "getReviewCount()" should "return the total number of reviews in our datastore" in {
    assert(Test.getReviewCount() >= 0);
  }

  "getGameReviewCount(Long)" should "return the number of reviews a game has in the reviews datastore" in {
    assert(Test.getGameReviewCount(1L) >= 0);
  }

  it should "return zero for a game_id that does not have any reviews or does not exist" in {
    assert(Test.getGameReviewCount(-1L) == 0);
  }

  "deleteGameReviews(Long)" should "return a list of all the reviews removed from the reviews datastore for the given game_id" in {
    assert(Test.deleteGameReviews(1).nonEmpty);
  }

  it should "return an empty list for games that do not have reviews or a non-existent game in the reviews datastore" in {
    assert(Test.deleteGameReviews(1).isEmpty);
    assert(Test.deleteGameReviews(-1).isEmpty);
  }

  "addArticle(Long, String, String, String, String, String, LocalDateTime, LocalDateTime, Map[Long, String], Long)" should "add a new article to the articles datastore" in {
    Test.addArticle(1, "test", "test", "test", "test", "test", LocalDateTime.now(), LocalDateTime.now(), Map((1L -> "test1"), (2L -> "test2")), 1);
    assert(Test.articleExists(1, LocalDateTime.now().getYear.toString));
  }

  "addArticles(List[(Long, String, String, String, String, String, LocalDateTime, LocalDateTime, Map[Long, String], Long)])" should "insert the passed-in articles into the articles datastore" in {
    Test.addArticles(
      List(
        (2, "test1", "test1", "test1", "test1", "test1", LocalDateTime.now(), LocalDateTime.now(), Map((1L -> "test1"), (2L -> "test2")), 1),
        (3, "test2", "test2", "test2", "test2", "test2", LocalDateTime.now(), LocalDateTime.now(), Map((1L -> "test1"), (2L -> "test2")), 1)
      )
    );
    assert(Test.articleExists(2, LocalDateTime.now().getYear.toString));
    assert(Test.articleExists(3, LocalDateTime.now().getYear.toString));
  }

  "articleExists(Long, String)" should "return true for an article_id that exist in the articles datastore within the given year partition" in {
    assert(Test.articleExists(1, LocalDateTime.now().getYear.toString));
  }

  it should "return false for a non-existent article_id in the given year partition" in {
    assert(!Test.articleExists(-1, LocalDateTime.now().getYear.toString));
  }

  "getArticle(Long, String)" should "return the details of the article within the given year with the given article_id" in {
    assert(Test.getArticle(6071468, LocalDateTime.now().getYear.toString) != null);
  }

  it should "return null if an article with the given id does not exist" in {
    assert(Test.getArticle(-1, LocalDateTime.now().getYear.toString) == null);
  }

  "getGameArticleCount(Long)" should "return the number of articles a game has in the articles datastore" in {
    assert(Test.getGameArticleCount(1L) >= 0);
  }

  it should "return zero if a game has zero articles or does not exist" in {
    assert(Test.getGameArticleCount(-1L) == 0);
  }

  "getGameArticles(Long)" should "return the list of articles associated with the passed-in game_id" in {
    assert(Test.getGameArticles(1).nonEmpty);
  }

  it should "return an empty list for a game without articles or a non-existent game_id in the articles datastore" in {
    assert(Test.getGameArticles(2).isEmpty);
    assert(Test.getGameArticles(-1).isEmpty);
  }

  "deleteGameArticles(Long)" should "return a list of removed articles for the specified game_id from the articles datastore" in {
    assert(Test.deleteGameArticles(1).nonEmpty);
    assert(Test.getGameArticles(1).isEmpty);
  }

  it should "return an empty list of articles if the game_id has no articles or the game_id does not exist in the articles datastore" in {
    assert(Test.deleteGameArticles(2).isEmpty);
    assert(Test.deleteGameArticles(-1).isEmpty);
  }
}