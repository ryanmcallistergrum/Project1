import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should
import java.time.LocalDateTime

class TestHiveDBManager extends AnyFlatSpec with should.Matchers {
  object Test extends HiveDBManager {
    override def connect(): SparkSession = super.connect();
    override def executeDML(spark: SparkSession, sql: String): Unit = super.executeDML(spark, sql);
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
    override def addGame(game_id: Int, name: String, release_date: LocalDateTime, deck: String, description: String, articles_api_url: String, reviews_api_url: String, avg_score: Double, article_count: Int, review_count: Int, genres: List[String], themes: List[String]): Unit = super.addGame(game_id, name, release_date, deck, description, articles_api_url, reviews_api_url, avg_score, article_count, review_count, genres, themes);
    override def addGames(games: List[(Int, String, LocalDateTime, String, String, String, String, Double, Int, Int, List[String], List[String])]): Unit = super.addGames(games);
    override def gameExists(game_id: Int, name: String): Boolean = super.gameExists(game_id, name);
    override def getGame(game_id: Int): (Int, String, LocalDateTime, String, String, String, String, Double, Int, Int, List[String], List[String]) = super.getGame(game_id);
    override def getLatestGames(): List[(Int, String, LocalDateTime, String, String, String, String, Double, Int, Int, List[String], List[String])] = super.getLatestGames();
    override def updateAvgScore(game_id: Int, newScore: Double): Double = super.updateAvgScore(game_id, newScore);
    override def updateArticleCount(game_id: Int, newArticleCount: Int): Int = super.updateArticleCount(game_id, newArticleCount);
    override def updateReviewCount(game_id: Int, newReviewCount: Int): Int = super.updateReviewCount(game_id, newReviewCount);
    override def deleteGame(game_id: Int): (Int, String, LocalDateTime, String, String, String, String, Double, Int, Int, List[String], List[String]) = super.deleteGame(game_id);
    override def deleteGames(game_ids: List[Int]): List[(Int, String, LocalDateTime, String, String, String, String, Double, Int, Int, List[String], List[String])] = super.deleteGames(game_ids);
    override def addReview(review_id: Int, authors: String, title: String, deck: String, lede: String, body: String, publish_date: LocalDateTime, update_date: LocalDateTime, score: Double, review_type: String, game_id: Int): Unit = super.addReview(review_id, authors, title, deck, lede, body, publish_date, update_date, score, review_type, game_id);
    override def addReviews(reviews: List[(Int, String, String, String, String, String, LocalDateTime, LocalDateTime, Double, String, Int)]): Unit = super.addReviews(reviews);
    override def reviewExists(review_id: Int): Boolean = super.reviewExists(review_id);
    override def getGameReviews(game_id: Int): List[(Int, String, String, String, String, String, LocalDateTime, LocalDateTime, Double, String, Int)] = super.getGameReviews(game_id);
    override def getLatestReviewDate(): LocalDateTime = super.getLatestReviewDate();
    override def getLatestGameReviewDate(game_id: Int): LocalDateTime = super.getLatestGameReviewDate(game_id);
    override def deleteGameReviews(game_id: Int): List[(Int, String, String, String, String, String, LocalDateTime, LocalDateTime, Double, String, Int)] = super.deleteGameReviews(game_id);
    override def addArticle(article_id: Int, authors: String, title: String, deck: String, lede: String, body: String, publish_date: LocalDateTime, update_date: LocalDateTime, categories: Map[Int, String], game_id: Int): Unit = super.addArticle(article_id, authors, title, deck, lede, body, publish_date, update_date, categories, game_id);
    override def addArticles(articles: List[(Int, String, String, String, String, String, LocalDateTime, LocalDateTime, Map[Int, String], Int)]): Unit = super.addArticles(articles);
    override def articleExists(article_id: Int): Boolean = super.articleExists(article_id);
    override def getGameArticles(game_id: Int): List[(Int, String, String, String, String, String, LocalDateTime, LocalDateTime, Map[Int, String], Int)] = super.getGameArticles(game_id);
    override def getLatestGameArticleDate(game_id: Int): LocalDateTime = super.getLatestGameArticleDate(game_id);
    override def deleteGameArticles(game_id: Int): List[(Int, String, String, String, String, String, LocalDateTime, LocalDateTime, Map[Int, String], Int)] = super.deleteGameArticles(game_id);
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
    assert(!Test.usernameExists("nonexistentusername"));
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
    val id : Int = Test.addUser("test02", "test02");
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
    Test.deleteQuery(1);
    assert(true);
  }

  "addGame(Int, String, LocalDateTime, String, String, String, String, Double, Int, Int, List[String], List[String]" should "insert a new game record into the games table if it does not already exist" in {
    Test.addGame(1, "test", LocalDateTime.now(), "test", "test", "http://www.nowhere.com", "http://www.nowhere.com", 0.0, 0, 0, List[String]("test01", "test02"), List[String]("test03", "test04"));
    assert(Test.gameExists(1, "test"));
  }

  "addGames(List[(Int, String, LocalDateTime, String, String, String, String, Double, Int, Int, List[String], List[String])])" should "insert new game records into the games table if they do not already exist" in {
    Test.addGames(List[(Int, String, LocalDateTime, String, String, String, String, Double, Int, Int, List[String], List[String])](
      (2, "test1", LocalDateTime.now(), "test", "test", "http://www.nowhere.com", "http://www.nowhere.com", 0.0, 0, 0, List[String](), List[String]()),
      (3, "test2", LocalDateTime.now(), "test", "test", "http://www.nowhere.com", "http://www.nowhere.com", 0.0, 0, 0, List[String](), List[String]()),
    ));
    assert(Test.gameExists(2, "test1"));
    assert(Test.gameExists(3, "test2"));
  }

  "gameExists(Int, String)" should "return true when its game_id and name parameters match a game in our datastore" in {
    assert(Test.gameExists(1, "test"));
  }

  it should "return false if the game_id does not match a game, then the name if the id matches, in the datastore" in {
    assert(!Test.gameExists(2, "test"));
    assert(!Test.gameExists(1, "test1"));
  }

  "getGame(Int)" should "return the game details for the specified game_id, assuming that it exists in our games datastore" in {
    val result : (Int, String, LocalDateTime, String, String, String, String, Double, Int, Int, List[String], List[String]) = Test.getGame(1);
      assert(result != null);
  }

  it should "return null if the game_id does not exist in the games datastore" in {
    val result : (Int, String, LocalDateTime, String, String, String, String, Double, Int, Int, List[String], List[String]) = Test.getGame(-1);
    assert(result == null);
  }

  "getLatestGames()" should "return the game details with the latest LocalDateTime of games that we have in the database" in {
    val result : List[(Int, String, LocalDateTime, String, String, String, String, Double, Int, Int, List[String], List[String])] = Test.getLatestGames();
    if (result.isEmpty)
      assert(result.isEmpty);
    else
      assert(result.nonEmpty);
  }

  "updateAvgScore(Int, Double)" should "update the game with the provided game_id's average score with the provided average score and return its previous value" in {
    assert(Test.updateAvgScore(1, 3.2) == 0.0);
    assert(Test.updateAvgScore(1, 0.0) == 3.2);
  }

  it should "should return the previous value if the provided value is greater than 10.0 or less than 0.0" in {
    assert(Test.updateAvgScore(1, -0.1) == 0.0);
    assert(Test.updateAvgScore(1, 10.01) == 0.0);
  }

  it should "return negative infinity for Double if the game id does not exist" in {
    assert(Test.updateAvgScore(-1, 2.0) == Double.NegativeInfinity);
  }

  "updateArticleCount(Int, Int)" should "update the game with the provided game_id's article count with the provided newArticleCount and return the previous article_count value" in {
    assert(Test.updateArticleCount(1, 1) == 0);
    assert(Test.updateArticleCount(1, 0) == 1);
  }

  it should "return the previous value if the new article count is less than 0" in {
    assert(Test.updateArticleCount(1, -1) == 0);
  }

  it should "return the smallest possible Int value if the game_id does not exist" in {
    assert(Test.updateArticleCount(-1, 2) == Int.MinValue);
  }

  "updateReviewCount(Int, Int)" should "update the game with the provided game_id's review count with the provided newReviewCount and return the previous review_count value" in {
    assert(Test.updateReviewCount(1, 1) == 0);
    assert(Test.updateReviewCount(1, 0) == 1);
  }

  it should "return the previous value if the new review count is less than 0" in {
    assert(Test.updateReviewCount(1, -1) == 0);
  }

  it should "return the smallest possible Int value if the game_id does not exist" in {
    assert(Test.updateReviewCount(-1, 2) == Int.MinValue);
  }

  "deleteGame(Int)" should "return the details of the game that is removed from the datastore" in {
    assert(Test.deleteGame(3) != null);
    assert(Test.getGame(3) == null);
  }

  it should "return null if no game was deleted" in {
    assert(Test.deleteGame(3) == null);
  }

  "deleteGames(List[Int])" should "return the game details of the games removed from the datastore" in {
    assert(Test.deleteGames(List(1, 2)).nonEmpty);
  }

  it should "return an empty list (or a list smaller than the number of game_ids) if games do not exist" in {
    assert(Test.deleteGames(List(-1, -2 ,-3)).isEmpty);
  }

  "addReview(Int, String, String, String, String, String, LocalDateTime, LocalDateTime, Double, String, Int)" should "add a new review to the reviews datastore" in {
    Test.addReview(1, "test", "test", "test", "test", "test", LocalDateTime.now(), LocalDateTime.now(), 0.0, "primary", 1);
    assert(Test.reviewExists(1));
  }

  "addreviews(List[(Int, String, String, String, String, String, LocalDateTime, LocalDateTime, Double, String, Int)]" should "add the passed-in reviews to the reviews datastore" in {
    Test.addReviews(
      List(
        (2, "test", "test", "test", "test", "test", LocalDateTime.now(), LocalDateTime.now(), 0.0, "secondary", 1),
        (3, "test", "test", "test", "test", "test", LocalDateTime.now(), LocalDateTime.now(), 0.0, "second take", 1)
      )
    );
    assert(Test.reviewExists(2));
    assert(Test.reviewExists(3));
  }

  "reviewExists(Int)" should "return true when it finds a review with the given review_id in our reviews datastore" in {
    assert(Test.reviewExists(1));
  }

  it should "return false if the review_id does not exist" in {
    assert(!Test.reviewExists(-1));
  }

  "getGameReviews(Int)" should "return a list of reviews for the given game_id, if any exist" in {
    val results : List[(Int, String, String, String, String, String, LocalDateTime, LocalDateTime, Double, String, Int)] = Test.getGameReviews(1);
    if (results.isEmpty)
      assert(results.isEmpty);
    else
      assert(results.nonEmpty);
  }

  "getLatestReviewDate()" should "return the latest publish_date in our reviews datastore, else null if none exist" in {
    val result : LocalDateTime = Test.getLatestReviewDate();
    if (result != null)
      assert(result != null);
    else
      assert(result == null);
  }

  "getLatestGameReviewDate(Int)" should "return the latest publish_date for a review for the given game_id" in {
    assert(Test.getLatestGameReviewDate(1) != null);
  }

  it should "return null if the game_id does not exist in reviews or the game does not have any reviews" in {
    assert(Test.getLatestGameReviewDate(-1) == null);
    assert(Test.getLatestGameReviewDate(2) == null);
  }

  "deleteGameReviews(Int)" should "return a list of all the reviews removed from the reviews datastore for the given game_id" in {
    assert(Test.deleteGameReviews(1).nonEmpty);
  }

  it should "return an empty list for games that do not have reviews or a non-existent game in the reviews datastore" in {
    assert(Test.deleteGameReviews(1).isEmpty);
    assert(Test.deleteGameReviews(-1).isEmpty);
  }

  "addArticle(Int, String, String, String, String, String, LocalDateTime, LocalDateTime, Map[Int, String], Int)" should "add a new article to the articles datastore" in {
    Test.addArticle(1, "test", "test", "test", "test", "test", LocalDateTime.now(), LocalDateTime.now(), Map((1 -> "test1"), (2 -> "test2")), 1);
    assert(Test.articleExists(1));
  }

  "addArticles(List[(Int, String, String, String, String, String, LocalDateTime, LocalDateTime, Map[Int, String], Int)])" should "insert the passed-in articles into the articles datastore" in {
    Test.addArticles(List(
      (2, "test1", "test1", "test1", "test1", "test1", LocalDateTime.now(), LocalDateTime.now(), Map((1 -> "test1"), (2 -> "test2")), 1),
      (3, "test2", "test2", "test2", "test2", "test2", LocalDateTime.now(), LocalDateTime.now(), Map((1 -> "test1"), (2 -> "test2")), 1)
    )
    );
    assert(Test.articleExists(2));
    assert(Test.articleExists(3));
  }

  "articleExists(Int)" should "return true for an article_id that exist in the articles datastore" in {
    assert(Test.articleExists(1));
  }

  it should "return false for a non-existent article_id" in {
    assert(!Test.articleExists(-1));
  }

  "getGameArticles(Int)" should "return the list of articles associated with the passed-in game_id" in {
    assert(Test.getGameArticles(1).nonEmpty);
  }

  it should "return an empty list for a game without articles or a non-existent game_id in the articles datastore" in {
    assert(Test.getGameArticles(2).isEmpty);
    assert(Test.getGameArticles(-1).isEmpty);
  }

  "getLatestGameArticleDate(Int)" should "return the latest publish_date for articles for the given game_id" in {
    assert(Test.getLatestGameArticleDate(1) != null);
  }

  it should "return null if the game_id does not have articles or the game_id does not exist in the articles datastore" in {
    assert(Test.getLatestGameArticleDate(2) == null);
    assert(Test.getLatestGameArticleDate(-1) == null);
  }

  "deleteGameArticles(Int)" should "return a list of removed articles for the specified game_id from the articles datastore" in {
    assert(Test.deleteGameArticles(1).nonEmpty);
    assert(Test.getLatestGameArticleDate(1) == null);
  }

  it should "return an empty list of articles if the game_id has no articles or the game_id does not exist in the articles datastore" in {
    assert(Test.deleteGameArticles(2).isEmpty);
    assert(Test.deleteGameArticles(-1).isEmpty);
  }
}