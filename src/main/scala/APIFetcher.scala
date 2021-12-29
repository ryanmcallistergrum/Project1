import ujson.Value

import java.io.IOException
import java.time.LocalDateTime
import scala.collection.mutable.ArrayBuffer

class APIFetcher extends GamespotAPI {
  private final val API_CONFIG_FILENAME : String = "apiDetails.txt";
  private var gameOffset : Long = 0;
  private var gameTotalResults : Long = 0;
  private var reviewOffset : Long = 0;
  private var reviewTotalResults : Long = 0;
  private var articleOffset : Long = 0;
  private var articleTotalResults : Long = 0;
  private var output : Boolean = false;
  private var summarize : Boolean = false;
  private var running : Boolean = true;

  def getLatest() : Unit = {
    while (running && getAPIKey().isEmpty)
      try {
        val map : Map[String, String] = getAPIConfig(API_CONFIG_FILENAME);
        setAPIKey(map("apiKey"));
      } catch {
        case n : NullPointerException => {
          n.printStackTrace();
          Thread.sleep(5000);
        }
        case io : IOException => {
          io.printStackTrace();
          Thread.sleep(5000);
        }
      }

    while (running) {
      gameOffset = if (HiveDBManager.getGameCount() == 0) 0 else HiveDBManager.getGameCount() - 1;
      init();
      selectEndpoint(true, false, false);
      setFormat(false, true, false);
      sortField("id", true);
      setOffset(gameOffset);
      var gameJson : Value = ujson.read(getResults());
      if (gameJson("error").str.equals("OK")) {
        val previousGameCount : Long = HiveDBManager.getGameCount();
        gameTotalResults = gameJson("number_of_total_results").num.toLong;
        while(gameOffset < gameTotalResults && running) {
          var i : Int = 0;
          while(i < gameJson("number_of_page_results").num.toInt && running) {
            getGame(gameJson("results")(i)("id").num.toLong);
            i += 1;
          }
          gameOffset += gameJson("number_of_page_results").num.toLong;

          if (running) {
            Thread.sleep(1000);
            init();
            selectEndpoint(true, false, false);
            setFormat(false, true, false);
            sortField("id", true);
            setOffset(gameOffset);
            gameJson = ujson.read(getResults());
            if (gameJson("error").str.equals("OK")) {
              outputFinding("Error while fetching from API! " + gameJson("error").str);
              running = false;
            }
          }
        }

        if (running && output && summarize && gameOffset - previousGameCount > 0)
          outputFinding(s"Added ${gameOffset - previousGameCount} new games.");

        outputFinding("Finished fetching the latest games! Waiting 1 hour before searching again...");
        Thread.sleep(60*60*60*1000);
      } else {
        outputFinding("Error while fetching from API! " + gameJson("error").str);
        running = false;
      }
    }
  }

  def getGame(game_id : Long) : Unit = {
    while (running && getAPIKey().isEmpty)
      try {
        val map : Map[String, String] = getAPIConfig(API_CONFIG_FILENAME);
        setAPIKey(map("apiKey"));
      } catch {
        case n : NullPointerException => {
          n.printStackTrace();
          Thread.sleep(5000);
        }
        case io : IOException => {
          io.printStackTrace();
          Thread.sleep(5000);
        }
      }

    init();
    selectEndpoint(true, false, false);
    setFormat(false, true, false);
    filterField("id", ":", game_id.toString);
    val gameJson : Value = ujson.read(getResults());
    if (gameJson("error").str.equals("OK")) {
      if(gameJson("number_of_total_results").num.toLong == 1 && running) {
        if (!HiveDBManager.gameExists(gameJson("results")(0)("id").num.toLong, gameJson("results")(0)("name").str.replace("'", "''"))) {
          try {
            val genres: ArrayBuffer[String] = ArrayBuffer();
            for (j: Int <- gameJson("results")(0)("genres").arr.indices)
              genres += gameJson("results")(0)("genres").arr(j)("name").str.replace("'", "''");

            val themes: ArrayBuffer[String] = ArrayBuffer();
            for (j: Int <- gameJson("results")(0)("themes").arr.indices)
              themes += gameJson("results")(0)("themes").arr(j)("name").str.replace("'", "''");


            HiveDBManager.addGame(
              gameJson("results")(0)("id").num.toLong,
              gameJson("results")(0)("name").str.replace("'", "''").replace("<", "\\<"),
              LocalDateTime.parse(gameJson("results")(0)("release_date").str.replace(" ", "T")),
              gameJson("results")(0)("deck").str.replace("'", "''").replace("<", "\\<"),
              gameJson("results")(0)("description").str.replace("'", "''").replace("<", "\\<"),
              gameJson("results")(0)("articles_api_url").str,
              gameJson("results")(0)("reviews_api_url").str,
              0.0,
              0L,
              0L,
              genres.toList,
              themes.toList
            );


            val game: (Long, String, LocalDateTime, String, String, String, String, Double, Long, Long, List[String], List[String]) = HiveDBManager.getGame(gameJson("results")(0)("id").num.toLong);
            if (running && output) {
              if (running && !summarize)
                outputFinding(s"Added new game: $game");
              else if (running)
                outputFinding(s"Added new game: ${"\""}${game._2}${"\""}.");
            }
          } catch {
            case nse : NoSuchElementException => {
              // We don't want the game if it is missing information.
            }
          }
        }

        val game: (Long, String, LocalDateTime, String, String, String, String, Double, Long, Long, List[String], List[String]) = HiveDBManager.getGame(gameJson("results")(0)("id").num.toLong);


        // Now get its reviews.
        getGameReviews(game._1);

        // Now get its articles.
        getGameArticles(game._1);

      } else {
        outputFinding(s"Game ID $game_id not found in Gamespot API! Stopping instance...");
        running = false;
      }
    } else {
      outputFinding("Error while fetching from API! " + gameJson("error").str);
      running = false;
    }
  }

  def getGame(game_name : String) : Unit = {
    while (running && getAPIKey().isEmpty)
      try {
        val map : Map[String, String] = getAPIConfig(API_CONFIG_FILENAME);
        setAPIKey(map("apiKey"));
      } catch {
        case n : NullPointerException => {
          n.printStackTrace();
          Thread.sleep(5000);
        }
        case io : IOException => {
          io.printStackTrace();
          Thread.sleep(5000);
        }
      }

    init();
    selectEndpoint(true, false, false);
    setFormat(false, true, false);
    filterField("name", ":", game_name);
    val gameJson : Value = ujson.read(getResults());
    if (gameJson("error").str.equals("OK")) {
      if(gameJson("number_of_total_results").num.toLong > 0 && running) {
        var i : Int = 0;
        while (i < gameJson("number_of_total_results").num.toLong && running) {
          if (gameJson("results")(i)("name").str.equals(game_name))
            getGame(gameJson("results")(i)("id").num.toLong);
          i += 1;
        }

        outputFinding(s"Finished fetching game ${"\""}$game_name${"\""}! Stopping instance...");
        running = false;
      } else {
        outputFinding(s"Game ${"\""}$game_name${"\""} not found in Gamespot API! Stopping instance...");
        running = false;
      }
    } else {
      outputFinding("Error while fetching from API! " + gameJson("error").str);
      running = false;
    }
  }

  def getGamesLike(game_name : String) : Unit = {
    while (running && getAPIKey().isEmpty)
      try {
        val map : Map[String, String] = getAPIConfig(API_CONFIG_FILENAME);
        setAPIKey(map("apiKey"));
      } catch {
        case n : NullPointerException => {
          n.printStackTrace();
          Thread.sleep(5000);
        }
        case io : IOException => {
          io.printStackTrace();
          Thread.sleep(5000);
        }
      }

    if (running) {
      init();
      selectEndpoint(true, false, false);
      setFormat(false, true, false);
      filterField("name", ":", game_name);
      var gameJson: Value = ujson.read(getResults());
      if (gameJson("error").str.equals("OK")) {
        var offset: Long = 0L;
        val totalResults: Long = gameJson("number_of_total_results").num.toLong;
        if (totalResults > 0) {
          while (offset < totalResults && running) {
            var i: Int = 0;
            while (i < gameJson("number_of_page_results").num.toLong && running) {
              getGame(gameJson("results")(i)("id").num.toLong)
              i += 1;
            }
            offset += gameJson("number_of_page_results").num.toLong;

            if (running) {
              Thread.sleep(1000);
              init();
              selectEndpoint(true, false, false);
              setFormat(false, true, false);
              filterField("name", ":", game_name);
              sortField("id", true);
              setOffset(offset);
              gameJson = ujson.read(getResults());
              if (!gameJson("error").str.equals("OK")) {
                outputFinding("Error while fetching from API! " + gameJson("error").str);
                running = false;
              }
            }
          }

          outputFinding(s"Finished fetching games like ${"\""}$game_name${"\""}! Stopping instance...");
          running = false;
        } else {
          outputFinding(s"Games like ${"\""}$game_name${"\""} not found in Gamespot API! Stopping instance...");
          running = false;
        }
      } else {
        outputFinding("Error while fetching from API! " + gameJson("error").str);
        running = false;
      }
    } else
      outputFinding("Instance stopped! Exiting...");
  }

  def getGamesBetween(startDate : LocalDateTime, endDate : LocalDateTime) : Unit = {
    while (running && getAPIKey().isEmpty)
      try {
        val map : Map[String, String] = getAPIConfig(API_CONFIG_FILENAME);
        setAPIKey(map("apiKey"));
      } catch {
        case n : NullPointerException => {
          n.printStackTrace();
          Thread.sleep(5000);
        }
        case io : IOException => {
          io.printStackTrace();
          Thread.sleep(5000);
        }
      }

    if (running) {
      init();
      selectEndpoint(true, false, false);
      setFormat(false, true, false);
      sortField("id", true);
      filterField("release_date", ":", startDate.toLocalDate + "|" + endDate.toLocalDate);
      var gameJson: Value = ujson.read(getResults());
      if (gameJson("error").str.equals("OK")) {
        var offset: Long = 0L;
        val totalResults: Long = gameJson("number_of_total_results").num.toLong;
        if (totalResults > 0) {
          while (offset < totalResults && running) {
            var i: Int = 0;
            while (i < gameJson("number_of_page_results").num.toLong && running) {
              getGame(gameJson("results")(i)("id").num.toLong)
              i += 1;
            }
            offset += gameJson("number_of_page_results").num.toLong;

            if (running) {
              Thread.sleep(1000);
              init();
              selectEndpoint(true, false, false);
              setFormat(false, true, false);
              filterField("release_date", ":", startDate.toLocalDate + "|" + endDate.toLocalDate);
              sortField("id", true);
              setOffset(offset);
              gameJson = ujson.read(getResults());
              if (!gameJson("error").str.equals("OK")) {
                outputFinding("Error while fetching from API! " + gameJson("error").str);
                running = false;
              }
            }
          }

          outputFinding(s"Finished fetching games between $startDate and $endDate! Stopping instance...");
          running = false;
        } else {
          outputFinding(s"No games found between $startDate and $endDate in Gamespot API! Stopping instance...");
          running = false;
        }
      } else {
        outputFinding("Error while fetching from API! " + gameJson("error").str);
        running = false;
      }
    } else
      outputFinding("Instance stopped! Exiting...");
  }

  def getGameReviews(game_id : Long) : Unit = {
    while (running && getAPIKey().isEmpty)
      try {
        val map : Map[String, String] = getAPIConfig(API_CONFIG_FILENAME);
        setAPIKey(map("apiKey"));
      } catch {
        case n : NullPointerException => {
          n.printStackTrace();
          Thread.sleep(5000);
        }
        case io : IOException => {
          io.printStackTrace();
          Thread.sleep(5000);
        }
      }

    if (running) {
      val game: (Long, String, LocalDateTime, String, String, String, String, Double, Long, Long, List[String], List[String]) = HiveDBManager.getGame(game_id);

      if (running && game != null) {
        Thread.sleep(1000)
        reviewOffset = HiveDBManager.getGameReviewCount(game._1);
        setURL(game._7, false, false, true);
        setFormat(false, true, false);
        sortField("id", true);
        var reviewJson: Value = ujson.read(getResults());
        reviewTotalResults = reviewJson("number_of_total_results").num.toLong;
        if (reviewJson("error").str.equals("OK")) {
          while (reviewOffset < reviewTotalResults && running) {
            var i: Int = 0;
            while (i < reviewJson("number_of_page_results").num.toInt && running) {
              try {
                HiveDBManager.addReview(
                  reviewJson("results")(i)("id").num.toLong,
                  reviewJson("results")(i)("authors").str.replace("'", "''").replace("<", "\\<"),
                  reviewJson("results")(i)("title").str.replace("'", "''").replace("<", "\\<"),
                  reviewJson("results")(i)("deck").str.replace("'", "''").replace("<", "\\<"),
                  reviewJson("results")(i)("lede").str.replace("'", "''").replace("<", "\\<"),
                  reviewJson("results")(i)("body").str.replace("'", "''").replace("<", "\\<"),
                  LocalDateTime.parse(reviewJson("results")(i)("publish_date").str.replace(" ", "T")),
                  LocalDateTime.parse(reviewJson("results")(i)("update_date").str.replace(" ", "T")),
                  reviewJson("results")(i)("score").str.toDouble,
                  reviewJson("results")(i)("review_type").str,
                  reviewJson("results")(i)("game")("id").num.toLong
                );

                if (output && !summarize) {
                  val review: (Long, String, String, String, String, String, LocalDateTime, LocalDateTime, Double, String, Long) = HiveDBManager.getReview(reviewJson("results")(i)("id").num.toLong);
                  outputFinding(s"Added new review for ${"\""}${game._2}${"\""}: $review");
                }
              } catch {
                case nse : NoSuchElementException => {
                  // We don't want the review if it is missing information.
                }
              }

              i += 1;
            }
            reviewOffset += reviewJson("number_of_page_results").num.toLong;

            if (running) {
              Thread.sleep(1000);
              setURL(game._7, false, false, true);
              setFormat(false, true, false);
              sortField("id", true);
              setOffset(reviewOffset);
              reviewJson = ujson.read(getResults());
              if (!reviewJson("error").str.equals("OK")) {
                outputFinding("Error while fetching from API! " + reviewJson("error").str);
                running = false;
              }
            }
          }

          if (running && output && summarize && reviewOffset - HiveDBManager.getPreviousGameReviewCount(game._1) > 0)
            outputFinding(s"Added ${reviewOffset - HiveDBManager.getPreviousGameReviewCount(game._1)} reviews to ${"\""}${game._2}${"\""}.");
          if (running && reviewOffset - HiveDBManager.getPreviousGameReviewCount(game_id) > 0) {
            HiveDBManager.updateReviewCount(game._1, HiveDBManager.getGameReviewCount(game._1))
            HiveDBManager.updateAvgScore(game._1, HiveDBManager.calculateAvgScore(game._1));
          }
        } else {
          outputFinding("Error while fetching from API! " + reviewJson("error").str);
          running = false;
        }
      } else {
        outputFinding(s"Game ID $game_id not found in our games table! Stopping instance...");
        running = false;
      }
    } else
      outputFinding("Instance stopped! Exiting...");
  }

  def getGameArticles(game_id : Long) : Unit = {
    while (running && getAPIKey().isEmpty)
      try {
        val map : Map[String, String] = getAPIConfig(API_CONFIG_FILENAME);
        setAPIKey(map("apiKey"));
      } catch {
        case n : NullPointerException => {
          n.printStackTrace();
          Thread.sleep(5000);
        }
        case io : IOException => {
          io.printStackTrace();
          Thread.sleep(5000);
        }
      }

    if (running) {
      val game: (Long, String, LocalDateTime, String, String, String, String, Double, Long, Long, List[String], List[String]) = HiveDBManager.getGame(game_id);

      if (running && game != null) {
        Thread.sleep(1000);
        articleOffset = HiveDBManager.getGameArticleCount(game._1);
        setURL(game._6, false, true, false);
        setFormat(false, true, false);
        sortField("id", true);
        var articleJson: Value = ujson.read(getResults());
        articleTotalResults = articleJson("number_of_total_results").num.toLong;
        if (articleJson("error").str.equals("OK")) {
          while (articleOffset < articleTotalResults && running) {
            var i: Int = 0;
            while (i < articleJson("number_of_page_results").num.toInt && running) {
              try {
                var categories: Map[Long, String] = Map();
                for (j: Int <- articleJson("results")(i)("categories").arr.indices)
                  categories += (
                    articleJson("results")(i)("categories").arr(j)("id").num.toLong ->
                      articleJson("results")(i)("categories").arr(j)("name").str.replace("'", "''")
                    );

                HiveDBManager.addArticle(
                  articleJson("results")(i)("id").num.toLong,
                  articleJson("results")(i)("authors").str.replace("'", "''").replace("<", "\\<"),
                  articleJson("results")(i)("title").str.replace("'", "''").replace("<", "\\<"),
                  articleJson("results")(i)("deck").str.replace("'", "''").replace("<", "\\<"),
                  articleJson("results")(i)("lede").str.replace("'", "''").replace("<", "\\<"),
                  articleJson("results")(i)("body").str.replace("'", "''").replace("<", "\\<"),
                  LocalDateTime.parse(articleJson("results")(i)("publish_date").str.replace(" ", "T")),
                  LocalDateTime.parse(articleJson("results")(i)("update_date").str.replace(" ", "T")),
                  categories,
                  game._1
                );

                if (output && !summarize) {
                  val article: (Long, String, String, String, String, String, LocalDateTime, LocalDateTime, Map[Long, String], Long) = HiveDBManager.getArticle(articleJson("results")(i)("id").num.toLong);
                  outputFinding(s"Added new article for ${"\""}${game._2}${"\""}: $article");
                }
              } catch {
                case nse : NoSuchElementException => {
                  // We don't want the article if it is missing information.
                }
              }

              i += 1;
            }
            articleOffset += articleJson("number_of_page_results").num.toLong;

            if (running) {
              Thread.sleep(1000);
              setURL(game._6, false, true, false);
              setFormat(false, true, false);
              sortField("id", true);
              setOffset(articleOffset)
              articleJson = ujson.read(getResults());
              if (!articleJson("error").str.equals("OK")) {
                outputFinding("Error while fetching from API! " + articleJson("error").str);
                running = false;
              }
            }
          }

          if (running && output && summarize && articleOffset - HiveDBManager.getPreviousGameArticleCount(game._1) > 0)
            outputFinding(s"Added ${articleOffset - HiveDBManager.getPreviousGameArticleCount(game._1)} articles to ${"\""}${game._2}${"\""}.");
          if (running && articleOffset - HiveDBManager.getPreviousGameArticleCount(game._1) > 0)
            HiveDBManager.updateArticleCount(game._1, HiveDBManager.getGameArticleCount(game._1));
        } else {
          outputFinding("Error while fetching from API! " + articleJson("error").str);
          running = false;
        }
      } else {
        outputFinding(s"Game ID $game_id not found in our games table! Stopping instance...");
        running = false;
      }
    } else
      outputFinding("Instance stopped! Exiting...");
  }

  def getAllReviews() : Unit = {
    while (running && getAPIKey().isEmpty)
      try {
        val map : Map[String, String] = getAPIConfig(API_CONFIG_FILENAME);
        setAPIKey(map("apiKey"));
      } catch {
        case n : NullPointerException => {
          n.printStackTrace();
          Thread.sleep(5000);
        }
        case io : IOException => {
          io.printStackTrace();
          Thread.sleep(5000);
        }
      }

    if (running) {
      val count : Long = HiveDBManager.getReviewCount();
      reviewOffset = if (count == 0) 0 else count;
      init();
      selectEndpoint(false, false, true);
      setFormat(false, true, false);
      sortField("id", true);
      setOffset(reviewOffset);
      var reviewJson: Value = ujson.read(getResults());
      reviewTotalResults = reviewJson("number_of_total_results").num.toLong;
      if (reviewJson("error").str.equals("OK")) {
        while (reviewOffset < reviewTotalResults && running) {
          var i: Int = 0;
          while (i < reviewJson("number_of_page_results").num.toInt && running) {
            try {
              HiveDBManager.addReview(
                reviewJson("results")(i)("id").num.toLong,
                reviewJson("results")(i)("authors").str.replace("'", "''").replace("<", "\\<"),
                reviewJson("results")(i)("title").str.replace("'", "''").replace("<", "\\<"),
                reviewJson("results")(i)("deck").str.replace("'", "''").replace("<", "\\<"),
                reviewJson("results")(i)("lede").str.replace("'", "''").replace("<", "\\<"),
                reviewJson("results")(i)("body").str.replace("'", "''").replace("<", "\\<"),
                LocalDateTime.parse(reviewJson("results")(i)("publish_date").str.replace(" ", "T")),
                LocalDateTime.parse(reviewJson("results")(i)("update_date").str.replace(" ", "T")),
                reviewJson("results")(i)("score").str.toDouble,
                reviewJson("results")(i)("review_type").str,
                reviewJson("results")(i)("game")("id").num.toLong
              );

              if (output && !summarize) {
                val review: (Long, String, String, String, String, String, LocalDateTime, LocalDateTime, Double, String, Long) = HiveDBManager.getReview(reviewJson("results")(i)("id").num.toLong);
                outputFinding(s"${reviewOffset + i}: Added new review for ${"\""}${reviewJson("results")(i)("game")("name").str}${"\""}: $review");
              }
            } catch {
              case nse : NoSuchElementException => {
                // We don't want the review if it is missing information.
              }
            }

            i += 1;
          }
          reviewOffset += reviewJson("number_of_page_results").num.toLong;

          if (running && reviewOffset - HiveDBManager.getPreviousGameReviewCount(reviewJson("results")(i)("id").num.toLong) > 0) {
            HiveDBManager.updateReviewCount(reviewJson("results")(i)("id").num.toLong, HiveDBManager.getGameReviewCount(reviewJson("results")(i)("id").num.toLong))
            HiveDBManager.updateAvgScore(reviewJson("results")(i)("id").num.toLong, HiveDBManager.calculateAvgScore(reviewJson("results")(i)("id").num.toLong));
          }

          if (running) {
            Thread.sleep(1000);
            init();
            selectEndpoint(false, false, true);
            setFormat(false, true, false);
            sortField("id", true);
            setOffset(reviewOffset);
            reviewJson = ujson.read(getResults());
            if (!reviewJson("error").str.equals("OK")) {
              outputFinding("Error while fetching from API! " + reviewJson("error").str);
              running = false;
            }
          }
        }

        outputFinding("Finished fetching all reviews! Stopping instance...");
        running = false;
      } else {
        outputFinding("Error while fetching from API! " + reviewJson("error").str);
        running = false;
      }
    } else
      outputFinding("Instance stopped! Exiting...");
  }

  def outputFindings() : Unit = {
    output = true;
  }

  protected def outputFinding(str : String) : Unit = {
    println(str);
  }

  def silence() : Unit = {
    output = false;
  }

  def summaryOutput() : Unit = {
    summarize = true;
  }

  def detailedOutput() : Unit = {
    summarize = false;
  }

  def stop() : Unit = {
    running = false;
  }
}