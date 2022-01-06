import org.apache.spark.sql.{DataFrame, Row, SparkSession}
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
    override def startupDB() : Unit = super.startupDB();
    override def createGamesCopy(table_name: String): Unit = super.createGamesCopy(table_name);
    override def createGameThemesCopy(table_name: String): Unit = super.createGameThemesCopy(table_name);
    override def createThemesCopy(table_name: String): Unit = super.createThemesCopy(table_name);
    override def createGameGenresCopy(table_name: String): Unit = super.createGameGenresCopy(table_name);
    override def createGenresCopy(table_name: String): Unit = super.createGenresCopy(table_name);
    override def createArticlesCopy(table_name: String): Unit = super.createArticlesCopy(table_name);
    override def createArticleAuthorsCopy(table_name: String): Unit = super.createArticleAuthorsCopy(table_name);
    override def createArticleCategoriesCopy(table_name: String): Unit = super.createArticleCategoriesCopy(table_name);
    override def createCategoriesCopy(table_name: String): Unit = super.createCategoriesCopy(table_name);
    override def createReviewsCopy(table_name: String): Unit = super.createReviewsCopy(table_name);
    override def createReviewAuthorsCopy(table_name: String): Unit = super.createReviewAuthorsCopy(table_name);
    override def createAuthorsCopy(table_name: String): Unit = super.createAuthorsCopy(table_name);
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
    override def getQuery(query_id: Int): String = super.getQuery(query_id);
    override def getQueries(): Map[Int, String] = super.getQueries();
    override def queryExists(query_id: Int): Boolean = super.queryExists(query_id);
    override def queryNameExists(query_name : String): Boolean = super.queryNameExists(query_name);
    override def showQuery(query_id: Int): Unit = super.showQuery(query_id);
    override def showQuery(query: String): Unit = super.showQuery(query);
    override def renameQuery(query_id: Int, newName: String): Unit = super.renameQuery(query_id, newName);
    override def saveQuery(user_id: Int, query_name: String, query: String): Unit = super.saveQuery(user_id, query_name, query);
    override def exportQueryResults(query_id: Int, filePath: String): Unit = super.exportQueryResults(query_id, filePath);
    override def deleteQuery(query_id: Int): Unit = super.deleteQuery(query_id);
    override def addGame(game_id: Long, name: String, release_date: LocalDateTime, deck: String, description: String, articles_api_url: String, reviews_api_url: String, article_count: Long, review_count: Long, genres: List[String], themes: List[String]): Unit = super.addGame(game_id, name, release_date, deck, description, articles_api_url, reviews_api_url, article_count, review_count, genres, themes);
    override def addGames(games: List[(Long, String, LocalDateTime, String, String, String, String, Long, Long, List[String], List[String])]): Unit = super.addGames(games);
    override def getGame(game_id: Long, year : String): (Long, String, LocalDateTime, String, String, String, String, Long, Long, List[String], List[String]) = super.getGame(game_id, year);
    override def getGame(game_name: String): (Long, String, LocalDateTime, String, String, String, String, Long, Long, List[String], List[String]) = super.getGame(game_name);
    override def gameExists(game_id: Long, year: String): Boolean = super.gameExists(game_id, year);
    override def getGamesBetween(startDate: LocalDateTime, endDate: LocalDateTime): List[(Long, String, LocalDateTime, String, String, String, String, Long, Long, List[String], List[String])] = super.getGamesBetween(startDate, endDate);
    override def getMaxGameBetween(startDate: LocalDateTime, endDate: LocalDateTime): (Long, String, LocalDateTime, String, String, String, String, Long, Long, List[String], List[String]) = super.getMaxGameBetween(startDate, endDate);
    override def getGameCountBetween(startDate: LocalDateTime, endDate: LocalDateTime): Long = super.getGameCountBetween(startDate, endDate);
    override def getMaxGame(): (Long, String, LocalDateTime, String, String, String, String, Long, Long, List[String], List[String]) = super.getMaxGame();
    override def getGames() : List[(Long, String, LocalDateTime, String, String, String, String, Long, Long, List[String], List[String])] = super.getGames();
    override def getGameCount(): Long = super.getGameCount();
    override def calculateAvgScore(game_id: Long): Double = super.calculateAvgScore(game_id);
    override def getPreviousGameArticleCount(game_id: Long, year : String): Long = super.getPreviousGameArticleCount(game_id, year);
    override def updateArticleCount(game_id: Long, year : String, newArticleCount: Long): Long = super.updateArticleCount(game_id, year, newArticleCount);
    override def getPreviousGameReviewCount(game_id: Long, year : String): Long = super.getPreviousGameReviewCount(game_id, year);
    override def updateReviewCount(game_id: Long, year : String, newReviewCount: Long): Long = super.updateReviewCount(game_id, year, newReviewCount);
    override def deleteGame(game_id: Long, year : String): (Long, String, LocalDateTime, String, String, String, String, Long, Long, List[String], List[String]) = super.deleteGame(game_id, year);
    override def deleteGames(games: Map[Long, String]): List[(Long, String, LocalDateTime, String, String, String, String, Long, Long, List[String], List[String])] = super.deleteGames(games);
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
    override def addAuthor(author: String): Long = super.addAuthor(author);
    override def authorExists(author: String): Boolean = super.authorExists(author);
    override def getAuthorId(author: String): Long = super.getAuthorId(author);
    override def getNextAuthorId(): Long = super.getNextAuthorId();
    override def addArticleAuthor(article_id: Long, author_id: Long): Unit = super.addArticleAuthor(article_id, author_id);
    override def getArticleAuthors(article_id: Long): String = super.getArticleAuthors(article_id);
    override def deleteArticleAuthors(article_id: Long): String = super.deleteArticleAuthors(article_id);
    override def addReviewAuthor(review_id: Long, author_id: Long): Unit = super.addReviewAuthor(review_id, author_id);
    override def getReviewAuthors(review_id: Long): String = super.getReviewAuthors(review_id);
    override def deleteReviewAuthors(review_id: Long): String = super.deleteReviewAuthors(review_id);
    override def addCategory(category_id: Long, category: String): Unit = super.addCategory(category_id, category);
    override def categoryExists(category_id: Long): Boolean = super.categoryExists(category_id);
    override def addArticleCategory(article_id: Long, category_id: Long): Unit = super.addArticleCategory(article_id, category_id);
    override def getArticleCategories(article_id: Long): Map[Long, String] = super.getArticleCategories(article_id);
    override def deleteArticleCategories(article_id: Long): Map[Long, String] = super.deleteArticleCategories(article_id);
    override def addGenre(genre: String): Long = super.addGenre(genre);
    override def genreExists(genre: String): Boolean = super.genreExists(genre);
    override def getGenreId(genre: String): Long = super.getGenreId(genre);
    override def getNextGenreId(): Long = super.getNextGenreId();
    override def addGameGenre(game_id: Long, genre_id: Long): Unit = super.addGameGenre(game_id, genre_id);
    override def getGameGenres(game_id: Long): List[String] = super.getGameGenres(game_id);
    override def deleteGameGenres(game_id: Long): List[String] = super.deleteGameGenres(game_id);
    override def addTheme(theme: String): Long = super.addTheme(theme);
    override def themeExists(theme: String): Boolean = super.themeExists(theme);
    override def addGameTheme(game_id: Long, theme_id: Long): Unit = super.addGameTheme(game_id, theme_id);
    override def getGameThemes(game_id: Long): List[String] = super.getGameThemes(game_id);
    override def deleteGameThemes(game_id: Long): List[String] = super.deleteGameThemes(game_id);
  }

  "randomcommands" should "only be used FOR TESTING ONLY" in {
    val df : DataFrame = Test.executeQuery(Test.connect(), "select article_id, authors, categories from p1.articles where authors not in ('', ' ') order by article_id");
    if (!df.isEmpty)
      for (row : Row <- df.collect()) {
        for (author : String <- row.getString(1).split(","))
          if (!Test.authorExists(author))
            Test.addArticleAuthor(row.getLong(0), Test.addAuthor(author));
          else
            Test.addArticleAuthor(row.getLong(0), Test.getAuthorId(author));
        val categories : Map[Long, String] = row.getAs[Map[Long, String]](2);
        for (category : Long <- categories.keys) {
          if (!Test.categoryExists(category))
            Test.addCategory(category, categories(category));
          Test.addArticleCategory(row.getLong(0), category);
        }
      }
    Test.createArticlesCopy("p1.articlesTemp");
    Test.executeDML(Test.connect(), "insert into table p1.articlesTemp select article_id, title, deck, lede, body, publish_date, update_date, game_id, year from p1.articles");
    Test.executeDML(Test.connect(), "drop table p1.articles");
    Test.executeDML(Test.connect(), "alter table p1.articlesTemp rename to articles");
  }

  "randomcommands2" should "only be used FOR TESTING" in {
    val spark : SparkSession = Test.connect();
    Test.createReviewsCopy("p1.reviewsTemp");
    Test.executeDML(spark, s"insert into p1.reviewsTemp select * from p1.reviewsByYear");
    Test.executeDML(spark, "drop table p1.reviews");
    Test.executeDML(spark, "drop table p1.reviewsByYear");
    Test.executeDML(spark, "alter table p1.reviewsTemp rename to reviews");

    Test.createArticlesCopy("p1.articlesTemp");
    Test.executeDML(spark, s"insert into p1.articlesTemp select * from p1.articlesByYear");
    Test.executeDML(spark, "drop table p1.articles");
    Test.executeDML(spark, "drop table p1.articlesByYear");
    Test.executeDML(spark, "alter table p1.articlesTemp rename to articles");
    Test.executeDML(spark, "truncate table p1.queries");
  }

  "Most Mentioned" should "show the most-mentioned game each year and month" in {
    Test.saveQuery(1, "Most Mentioned", "select g.name, max_counts.max_num as mentions, max_counts.year, max_counts.month from (select max(counts.num) as max_num, counts.year, counts.month from (select game_id, count(game_id) as num, year, date_format(publish_date, \\'MM\\') as month from p1.articles group by year, date_format(publish_date, \\'MM\\'), game_id order by year desc, month desc, num desc, game_id asc) counts group by counts.year, counts.month order by counts.year desc, counts.month desc) max_counts, (select game_id, count(game_id) as num, year, date_format(publish_date, \\'MM\\') as month from p1.articles group by year, date_format(publish_date, \\'MM\\'), game_id order by year desc, month desc, num desc, game_id asc) counts, p1.games g where g.game_id = counts.game_id and counts.num = max_counts.max_num and counts.month = max_counts.month and counts.year = max_counts.year and max_counts.year >= 2011 order by max_counts.year desc, max_counts.month desc, max_counts.max_num desc, g.name asc"
    );
    /*val spark : SparkSession = Test.connect();
    val df : DataFrame = Test.executeQuery(spark,
      "select g.name, max_counts.max_num as mentions, max_counts.year, max_counts.month " +
      "from (" +
        "select max(counts.num) as max_num, counts.year, counts.month " +
          "from (" +
            "select game_id, count(game_id) as num, year, date_format(publish_date, 'MM') as month " +
            "from p1.articles " +
            "group by year, date_format(publish_date, 'MM'), game_id " +
            "order by year desc, month desc, num desc, game_id asc" +
          ") counts " +
        "group by counts.year, counts.month " +
        "order by counts.year desc, counts.month desc" +
      ") max_counts, (" +
        "select game_id, count(game_id) as num, year, date_format(publish_date, 'MM') as month " +
        "from p1.articles " +
        "group by year, date_format(publish_date, 'MM'), game_id " +
        "order by year desc, month desc, num desc, game_id asc" +
      ") counts, p1.games g " +
    "where g.game_id = counts.game_id " +
      "and counts.num = max_counts.max_num " +
      "and counts.month = max_counts.month " +
      "and counts.year = max_counts.year " +
      "and max_counts.year >= 2011 " +
    "order by max_counts.year desc, max_counts.month desc, max_counts.max_num desc, g.name asc"
    );
    df.show(Int.MaxValue, false);
    assert(true);*/
  }

  "Least Mentioned" should "show the least-mentioned games in articles each year" in {
    Test.saveQuery(1,"Least Mentioned", "select g.name, min_counts.min_num as mentions, max(min_counts.year) as year from (select min(counts.num) as min_num, counts.year from (select game_id, count(game_id) as num, year from p1.articles group by year, game_id order by year desc, num desc, game_id asc) counts group by counts.year order by counts.year desc) min_counts, (select game_id, count(game_id) as num, year from p1.articles group by year, game_id order by year desc, num desc, game_id asc) counts, p1.games g where g.game_id = counts.game_id and counts.num = min_counts.min_num and counts.year = min_counts.year and min_counts.year >= 2011 group by min_counts.min_num, g.name order by max(min_counts.year) desc, min_counts.min_num desc, g.name asc"
    );
    /*val spark : SparkSession = Test.connect();
    val df : DataFrame = Test.executeQuery(spark,
      "select g.name, min_counts.min_num as mentions, max(min_counts.year) as year " +
        "from (" +
          "select min(counts.num) as min_num, counts.year " +
          "from (" +
            "select game_id, count(game_id) as num, year " +
            "from p1.articles " +
            "group by year, game_id " +
            "order by year desc, num desc, game_id asc" +
          ") counts " +
          "group by counts.year " +
          "order by counts.year desc" +
        ") min_counts, (" +
          "select game_id, count(game_id) as num, year " +
          "from p1.articles " +
          "group by year, game_id " +
          "order by year desc, num desc, game_id asc" +
        ") counts, p1.games g " +
        "where g.game_id = counts.game_id " +
        "and counts.num = min_counts.min_num " +
        "and counts.year = min_counts.year " +
        "and min_counts.year >= 2011 " +
        "group by min_counts.min_num, g.name " +
        "order by max(min_counts.year) desc, min_counts.min_num desc, g.name asc"
    );
    df.show(Int.MaxValue, false);
    assert(true);*/
  }

  "Articles With Cheats" should "show articles with cheats mentioned somewhere" in {
    Test.saveQuery(1,"Articles With Cheats", "select distinct g.name, a.title, regexp_extract(a.body, \\'(Cheat(s|er|ers|ing)?([^\\.]|.net)?\\.|[A-Z][^A-Z\\.]+cheat(s|er|ers|ing)?([^\\.]|.net)?\\.)\\', 1) as sentence from p1.games g, p1.articles a where g.game_id = a.game_id and a.body rlike \\'[Cc]heat(s|er|ers|ing)?\\' order by g.name, a.title");
    /*val spark : SparkSession = Test.connect();
    val df : DataFrame = Test.executeQuery(spark,
      "select distinct g.name, a.title, regexp_extract(a.body, '(Cheat(s|er|ers|ing)?([^\\.]|.net)+\\.|[A-Z][^A-Z\\.]+cheat(s|er|ers|ing)?([^\\.]|.net)?\\.)', 1) as sentence " +
      "from p1.games g, p1.articles a " +
      "where g.game_id = a.game_id " +
        "and a.body rlike '[Cc]heat(s|er|ers|ing)?' " +
      "order by g.name, a.title"
    );
    df.show(Int.MaxValue, false);
    assert(true);*/
  }

  "Games, Articles, and Reviews From 2021" should "show how many games, articles, and reviews we would delete from a given period" in {
    Test.saveQuery(1, "Games, Articles, and Reviews From 2021","select gameCount.num as games, articleCount.num as articles, reviewCount.num as reviews from (select count(g.game_id) as num from p1.games g where g.release_date between \\'2021-01-01\\' and \\'2021-12-31\\') gameCount, (select count(a.article_id) as num from p1.articles a where a.publish_date between \\'2021-01-01\\' and \\'2021-12-31\\' and a.year between year(\\'2021-01-01\\') and year(\\'2021-12-31\\')) articleCount, (select count(r.review_id) as num from p1.reviews r where r.publish_date between \\'2021-01-01\\' and \\'2021-12-31\\' and r.year between year(\\'2021-01-01\\') and year(\\'2021-12-31\\')) reviewCount");
    /*val spark : SparkSession = Test.connect();
    val df : DataFrame = Test.executeQuery(spark,
      "select games.num, articles.num, reviews.num " +
      "from (" +
        "select count(g.game_id) as num " +
        "from p1.games g " +
        "where g.release_date between '2021-01-01' and '2021-12-31'" +
      ") games, (" +
        "select count(a.article_id) as num " +
        "from p1.articles a " +
        "where a.publish_date between '2021-01-01' and '2021-12-31' " +
        "and a.year between year('2021-01-01') and year('2021-12-31')" +
      ") articles, (" +
        "select count(r.review_id) as num " +
        "from p1.reviews r " +
        "where r.publish_date between '2021-01-01' and '2021-12-31' " +
        "and r.year between year('2021-01-01') and year('2021-12-31')" +
      ") reviews "
    );
    df.show(Int.MaxValue, false);
    assert(true);*/
  }

  "Reviews Over Time" should "show the changes in review ratings 1, 5, 15, and 30 days out from an article" in {
    Test.saveQuery(1, "Review Median Per Article Over Time", "select distinct g.name, a.title, oneDay.rating as 1Day, fiveDay.rating as 5Days, fifteenDay.rating as 15Days, thirtyDay.rating as 30Days from (select a.game_id, a.title, percentile_approx(r.score, 0.5) as rating, a.year from p1.reviews r, p1.articles a where r.publish_date between a.publish_date and date_add(a.publish_date, 1) and a.game_id = r.game_id and r.year between year(a.publish_date) and year(date_add(a.publish_date, 1)) group by a.game_id, a.title, a.year order by a.game_id asc, a.year desc) oneDay, (select a.game_id, a.title, percentile_approx(r.score, 0.5) as rating, a.year from p1.reviews r, p1.articles a where r.publish_date between a.publish_date and date_add(a.publish_date, 5) and a.game_id = r.game_id and r.year between year(a.publish_date) and year(date_add(a.publish_date, 5)) group by a.game_id, a.title, a.year order by a.game_id asc, a.year desc) fiveDay, (select a.game_id, a.title, percentile_approx(r.score, 0.5) as rating, a.year from p1.reviews r, p1.articles a where r.publish_date between a.publish_date and date_add(a.publish_date, 15) and a.game_id = r.game_id and r.year between year(a.publish_date) and year(date_add(a.publish_date, 15)) group by a.game_id, a.title, a.year order by a.game_id asc, a.year desc) fifteenDay, (select a.game_id, a.title, percentile_approx(r.score, 0.5) as rating, a.year from p1.reviews r, p1.articles a where r.publish_date between a.publish_date and date_add(a.publish_date, 30) and a.game_id = r.game_id and r.year between year(a.publish_date) and year(date_add(a.publish_date, 30)) group by a.game_id, a.title, a.year order by a.game_id asc, a.year desc) thirtyDay, p1.games g, p1.articles a where g.game_id = a.game_id and g.game_id = oneDay.game_id and g.game_id = fiveDay.game_id and g.game_id = fifteenDay.game_id and g.game_id = thirtyDay.game_id and a.title = oneDay.title and a.title = fiveDay.title and a.title = fifteenDay.title and a.title = thirtyDay.title and a.year = oneDay.year and a.year = fiveDay.year and a.year = fifteenDay.year and a.year = thirtyDay.year order by g.name asc");
    /*val spark : SparkSession = Test.connect();
    val df : DataFrame = Test.executeQuery(spark,
      "select distinct g.name, a.title, oneDay.rating as 1Day, fiveDay.rating as 5Days, fifteenDay.rating as 15Days, thirtyDay.rating as 30Days " +
      "from (" +
        "select a.game_id, a.title, percentile_approx(r.score, 0.5) as rating, a.year " +
        "from p1.reviews r, p1.articles a " +
        "where r.publish_date between a.publish_date and date_add(a.publish_date, 1) " +
          "and a.game_id = r.game_id " +
          "and r.year between year(a.publish_date) and year(date_add(a.publish_date, 1)) " +
        "group by a.game_id, a.title, a.year " +
        "order by a.game_id asc, a.year desc" +
      ") oneDay, (" +
        "select a.game_id, a.title, percentile_approx(r.score, 0.5) as rating, a.year " +
        "from p1.reviews r, p1.articles a " +
        "where r.publish_date between a.publish_date and date_add(a.publish_date, 5) " +
          "and a.game_id = r.game_id " +
          "and r.year between year(a.publish_date) and year(date_add(a.publish_date, 5)) " +
        "group by a.game_id, a.title, a.year " +
        "order by a.game_id asc, a.year desc" +
      ") fiveDay, (" +
        "select a.game_id, a.title, percentile_approx(r.score, 0.5) as rating, a.year " +
        "from p1.reviews r, p1.articles a " +
        "where r.publish_date between a.publish_date and date_add(a.publish_date, 15) " +
          "and a.game_id = r.game_id " +
          "and r.year between year(a.publish_date) and year(date_add(a.publish_date, 15)) " +
        "group by a.game_id, a.title, a.year " +
        "order by a.game_id asc, a.year desc" +
      ") fifteenDay, (" +
        "select a.game_id, a.title, percentile_approx(r.score, 0.5) as rating, a.year " +
        "from p1.reviews r, p1.articles a " +
        "where r.publish_date between a.publish_date and date_add(a.publish_date, 30) " +
          "and a.game_id = r.game_id " +
          "and r.year between year(a.publish_date) and year(date_add(a.publish_date, 30)) " +
        "group by a.game_id, a.title, a.year " +
        "order by a.game_id asc, a.year desc" +
      ") thirtyDay, p1.games g, p1.articles a " +
      "where g.game_id = a.game_id " +
        "and g.game_id = oneDay.game_id " +
        "and g.game_id = fiveDay.game_id " +
        "and g.game_id = fifteenDay.game_id " +
        "and g.game_id = thirtyDay.game_id " +
        "and a.title = oneDay.title " +
        "and a.title = fiveDay.title " +
        "and a.title = fifteenDay.title " +
        "and a.title = thirtyDay.title " +
        "and a.year = oneDay.year " +
        "and a.year = fiveDay.year " +
        "and a.year = fifteenDay.year " +
        "and a.year = thirtyDay.year " +
      "order by g.name asc"
    );
    df.show(Int.MaxValue, false);
    assert(true);*/
  }

  "New Releases, Reviews, and Articles Since 2021" should "show games that have been newly released along with games that have had new reviews and articles, and how many from a given date, since the start of 2021" in {
    Test.saveQuery(1, "New Releases, Reviews, and Articles Since 2021", "select g.name, (case when g.release_date <= \\'2021-01-01\\' then false else true end) as released, (select count(publish_date) from p1.articles where publish_date >= \\'2021-01-01\\' and game_id = g.game_id and year >= year(\\'2021-01-01\\')) as articleCount, (select count(publish_date) from p1.reviews where publish_date >= \\'2021-01-01\\' and game_id = g.game_id and year >= year(\\'2021-01-01\\')) as reviewCount from p1.games g where (select count(publish_date) from p1.articles where publish_date >= \\'2021-01-01\\' and game_id = g.game_id and year >= year(\\'2021-01-01\\')) > 0 or (select count(publish_date) from p1.reviews where publish_date >= \\'2021-01-01\\' and game_id = g.game_id and year >= year(\\'2021-01-01\\')) > 0 or (case when g.release_date <= \\'2021-01-01\\' then false else true end) = true order by g.name");
    /*val spark : SparkSession = Test.connect();
    val df : DataFrame = Test.executeQuery(spark,
      "select g.name, (case when g.release_date <= '2021-01-01' then false else true end) as released, " +
        "(select count(publish_date) from p1.articles where publish_date >= '2021-01-01' and game_id = g.game_id and year >= year('2021-01-01')) as articleCount, " +
        "(select count(publish_date) from p1.reviews where publish_date >= '2021-01-01' and game_id = g.game_id and year >= year('2021-01-01')) as reviewCount " +
      "from p1.games g " +
      "where (select count(publish_date) from p1.articles where publish_date >= '2021-01-01' and game_id = g.game_id and year >= year('2021-01-01')) > 0 " +
        "or (select count(publish_date) from p1.reviews where publish_date >= '2021-01-01' and game_id = g.game_id and year >= year('2021-01-01')) > 0 " +
        "or (case when g.release_date <= '2021-01-01' then false else true end) = true " +
      "order by g.name"
    );
    df.show(Int.MaxValue, false);
    assert(true);*/
  }

  "executeQuery(SparkSession, String)" should "be for TESTING ONLY!" in {
    val df : DataFrame = Test.executeQuery(Test.connect(), "select * from p1.articles where game_id = (select game_id from p1.games where name = 'Duke Nukem Forever') order by publish_date");
    df.show(Int.MaxValue, true);
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

  "startupDB()" should "perform the initial slow connect to the datastore" in {
    Test.startupDB();
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

  "getQuery(Int)" should "return the actual SQL query stored in the queries table with the given query_id" in {
    val queries : Map[Int, String] = Test.getQueries();
    if (queries.nonEmpty)
      assert(Test.getQuery(queries.keys.head).nonEmpty);
    else
      assert(false);
  }

  "getQueries()" should "return a list of query IDs and names from the queries table" in {
    if (Test.getQueries().isEmpty)
      assert(true);
    else
      assert(Test.getQueries().nonEmpty);
  }

  "queryExists(Int)" should "return whether a query with the given query_id exists in our datastore" in {
    val queries : Map[Int, String] = Test.getQueries();
    if (queries.nonEmpty)
      assert(Test.queryExists(queries.keys.head));
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

  "renameQuery(Int, String)" should "rename the query with the given query_id to the given newQuery name" in {
    assert(!Test.queryNameExists("test"));
    Test.saveQuery(1, "test", "test");
    assert(Test.queryNameExists("test"));
    Test.renameQuery(Test.getQueries().find(q => q._2.equals("test")).get._1, "test02");
    assert(Test.queryNameExists("test02"));
    Test.deleteQuery(Test.getQueries().find(q => q._2.equals("test02")).get._1);
    assert(!Test.queryNameExists("test02"));
  }

  "saveQuery(Int, String, String)" should "save the passed-in query and user information to the queries table" in {
    assert(Test.getQueries().isEmpty);
    Test.saveQuery(Test.authenticate("admin", "admin", true), "test", "select * from p1.users");
    assert(Test.getQueries().nonEmpty);
  }

  "exportQueryResults(Int, String)" should "save the results of the query with the given query_id to the file located in the given filePath String" in {
    Test.exportQueryResults(Test.getQueries().head._1, "test.json");
  }

  "deleteQuery(Int)" should "delete the query with the passed-in query_id from the queries table" in {
    val name : String = Test.getQueries().find(p => p._1 == 1).get._2;
    Test.deleteQuery(1);
    assert(!Test.queryNameExists(name));
  }

  "addGame(Long, String, LocalDateTime, String, String, String, String, Double, Long, Long, List[String], List[String]" should "insert a new game record into the games table if it does not already exist" in {
    Test.addGame(1, "test", LocalDateTime.now(), "test", "test", "http://www.nowhere.com", "http://www.nowhere.com", 0, 0, List[String]("test01", "test02"), List[String]("test03","test04"));
    assert(Test.gameExists(1, LocalDateTime.now().getYear.toString));
  }

  "addGames(List[(Long, String, LocalDateTime, String, String, String, String, Double, Long, Long, List[String], List[String])])" should "insert new game records into the games table if they do not already exist" in {
    Test.addGames(List[(Long, String, LocalDateTime, String, String, String, String, Long, Long, List[String], List[String])](
      (2, "test1", LocalDateTime.now(), "test", "test", "http://www.nowhere.com", "http://www.nowhere.com", 0, 0, List[String](), List[String]()),
      (3, "test2", LocalDateTime.now(), "test", "test", "http://www.nowhere.com", "http://www.nowhere.com", 0, 0, List[String](), List[String]()),
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
    val result : (Long, String, LocalDateTime, String, String, String, String, Long, Long, List[String], List[String]) = Test.getGame(1, LocalDateTime.now().getYear.toString);
    assert(result != null);
  }

  it should "return null if the game_id does not exist in the games datastore" in {
    val result : (Long, String, LocalDateTime, String, String, String, String, Long, Long, List[String], List[String]) = Test.getGame(-1, LocalDateTime.now().getYear.toString);
    assert(result == null);
  }

  "getGame(String)" should "return the game details for the specified game_name, assuming that it exists in our games datastore" in {
    val result : (Long, String, LocalDateTime, String, String, String, String, Long, Long, List[String], List[String]) = Test.getGame("test");
    assert(result != null);
  }

  it should "return null if the game_id does not exist in the games datastore" in {
    val result : (Long, String, LocalDateTime, String, String, String, String, Long, Long, List[String], List[String]) = Test.getGame("nonexistentgame");
    assert(result == null);
  }

  "getGamesBetween(LocalDateTime, LocalDateTime)" should "return a list of games that exist between the start and end dates" in {
    val result : List[(Long, String, LocalDateTime, String, String, String, String, Long, Long, List[String], List[String])] = Test.getGamesBetween(LocalDateTime.parse("2021-12-01T00:00:00"), LocalDateTime.parse("2021-12-31T23:59:59"));
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
    val games : List[(Long, String, LocalDateTime, String, String, String, String, Long, Long, List[String], List[String])] = Test.getGames();
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

  "addAuthor(String)" should "add the author with the given name to the authors table" in {
    Test.addAuthor("test");
    assert(Test.authorExists("test"));
  }

  "authorExists(String)" should "return true or false if the given author exists in our authors table" in {
    assert(Test.authorExists("test"));
    assert(!Test.authorExists("nonexistentauthor"));
  }

  "getAuthorId(String)" should "return the author_id for the given author" in {
    assert(Test.getAuthorId("test") == 1);
  }

  it should "return zero if the author does not exist" in {
    assert(Test.getAuthorId("nonexistentauthor") == 0);
  }

  "getNextAuthorId()" should "return the next assignable author_id based in the maximum author_id value in the authors table" in {
    assert(Test.getNextAuthorId() == 2);
  }

  it should "return 1 if no authors exist in the table" in {
    Test.executeDML(Test.connect(), "truncate table p1.authors");
    assert(Test.getNextAuthorId() == 1);
  }

  "addArticleAuthor(Long, Long)" should "add a record for the given article_id and author_id to the articleAuthors table" in {
    Test.addArticleAuthor(1L, 1L);
  }

  "getArticleAuthors(Long)" should "return the concatenated String representation of the authors for the given article_id as 'Name1; Name2; ...'" in {
    assert(Test.getArticleAuthors(1L).nonEmpty);
  }

  "deleteArticleAuthors(Long)" should "remove the article-author mappings from articleAuthors for the given article_id and return them in the concatenated String representation" in {
    assert(Test.deleteArticleAuthors(1L).nonEmpty);
    assert(Test.deleteArticleAuthors(1L).isEmpty);
  }

  "addReviewAuthor(Long, Long)" should "add a review-author mapping to the reviewAuthors table for the given review_id and author_id" in {
    Test.addReviewAuthor(1L, 1L);
  }

  "getReviewAuthors(Long)" should "return the concatenated String representation of the authors for hte given review_id as 'Name1; Name2; ...'" in {
    assert(Test.getReviewAuthors(1L).nonEmpty);
  }

  "deleteReviewAuthors(Long)" should "remove the review-author mappings from reviewAuthors for the given review_id and return them in teh concatenated String representation" in {
    assert(Test.deleteReviewAuthors(1L).nonEmpty);
    assert(Test.deleteReviewAuthors(1L).isEmpty);
  }

  "addCategory(Long, String)" should "add an entry for the given category_id and category into the categories table" in {
    Test.addCategory(1L, "test");
  }

  "categoryExists(Long)" should "return whether the given category_id exists in the categories table" in {
    assert(Test.categoryExists(1L));
    Test.executeDML(Test.connect(), "truncate table p1.categories");
  }

  "addArticleCategory(Long, Long)" should "add an article-category association mapping to the articleCategories table" in {
    Test.addArticleCategory(1L, 1L);
  }

  "getArticleCategories(Long)" should "return a Map[Long, String] containing the categories associated with the given article_id" in {
    assert(Test.getArticleCategories(1L).nonEmpty);
  }

  "deleteArticleCategories(Long)" should "remove the article-category associations in articleCatgories and return the removed entries in a Map[Long, String] structure" in {
    assert(Test.deleteArticleCategories(1L).nonEmpty);
    assert(Test.deleteArticleCategories(1L).isEmpty);
  }

  "addGenre(String)" should "add the given genre to the genres table" in {
    Test.addGenre("test");
  }

  "genreExists(String)" should "return whether the given genre exists in the genres table" in {
    assert(Test.genreExists("test"));
    Test.executeDML(Test.connect(), "truncate table p1.genres");
  }

  "addGameGenre" should "add a game-genre association mapping to the gameGenres table" in {
    Test.addGameGenre(1L, 1L);
  }

  "getGameGenres(Long)" should "return a Map[long, String] structure containing the genre details associated with the given game_id" in {
    assert(Test.getGameGenres(1L).nonEmpty);
  }

  "deleteGameGenres(Long)" should "remove entries in gameGenres for the given game_id and return them in a Map[Long, String] structure" in {
    assert(Test.deleteGameGenres(1L).nonEmpty);
    assert(Test.deleteGameGenres(1L).isEmpty);
  }

  "addTheme(String)" should "add an entry to the themes table for the given theme" in {
    Test.addTheme("test");
  }

  "themeExists(String)" should "return whether the given theme exists in the themes table" in {
    assert(Test.themeExists("test"));
    Test.executeDML(Test.connect(), "truncate table p1.themes");
  }

  "addGameTheme(Long, Long)" should "add the game_id-theme_id association to the gameThemes table" in {
    Test.addGameTheme(1L, 1L);
  }

  "getGameThemes(Long)" should "return a List[String] structure containing the theme details for the given game_id's game-theme mappings in gameThemes" in {
    assert(Test.getGameThemes(1L).nonEmpty);
  }

  "deleteGameThemes(Long)" should "remove all entries associated with the given game_id from gameThemes and return their details in a List[String] structure" in {
    assert(Test.deleteGameThemes(1L).nonEmpty);
    assert(Test.deleteGameThemes(1L).isEmpty);
  }
}