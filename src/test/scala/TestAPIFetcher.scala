import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

import java.time.LocalDateTime

class TestAPIFetcher extends AnyFlatSpec with should.Matchers {
  object Test extends APIFetcher {
    override def getLatest(): Unit = super.getLatest();
    override def getGame(game_id : Long) : Unit = super.getGame(game_id);
    override def getGame(game_name: String): Unit = super.getGame(game_name);
    override def getGamesLike(game_name: String): Unit = super.getGamesLike(game_name);
    override def getGamesBetween(startDate: LocalDateTime, endDate: LocalDateTime): Unit = super.getGamesBetween(startDate, endDate);
    override def getGameReviews(game_id: Long, year : String): Unit = super.getGameReviews(game_id, year);
    override def getGameArticles(game_id: Long, year : String): Unit = super.getGameArticles(game_id, year);
    override def getAllReviews(): Unit = super.getAllReviews()
    override def getAllGameThemes(): Unit = super.getAllGameThemes();
    override def getAllGameGenres(): Unit = super.getAllGameGenres();
    override def outputFindings(): Unit = super.outputFindings();
    override def outputFinding(str: String): Unit = super.outputFinding(str);
    override def silence(): Unit = super.silence();
    override def summaryOutput(): Unit = super.summaryOutput();
    override def detailedOutput(): Unit = super.detailedOutput();
    override def stop(): Unit = super.stop();
  }

  "getLatest()" should "fetch the latest games, starting from the latest game(s) in our games datastore" in {
    val test : APIFetcher = Test;
    val t : Thread = new Thread {
      override def run(): Unit = {
        test.outputFindings();
        test.getLatest();
      }
    };
    t.start();
    Thread.sleep(60*1000);

    println("Stopping reporting...");
    test.silence();
    Thread.sleep(10*1000);

    println("Starting reporting...");
    test.outputFindings();
    Thread.sleep(60*1000);

    println("Switching reporting to summarized...");
    test.summaryOutput();
    Thread.sleep(60*1000);

    println("Stopping APIFetcher.")
    test.stop();
    Thread.sleep(5*1000);
  }

  "getGame(Long)" should "go out and fetch missing game details, articles, and reviews for the game with the associated game_id" in {
    Test.outputFindings();
    for(i : Long <- 2L to 22L)
      Test.getGame(i);
  }

  "getGame(String)" should "go out and fetch missing game details, articles, and reviews for the game with the associated name" in {
    Test.outputFindings();
    Test.getGame("Uprising 2: Lead and Destroy");
  }

  "getGamesLike(String)" should "go out and fetch missing game details, articles, and reviews for game with a similar name" in {
    Test.outputFindings();
    Test.getGamesLike("Tomb Raider");
  }

  "getGamesBetween(LocalDateTime, LocalDateTime)" should "go out and fetch missing game details, articles, and reviews for the games whose release_date lie between startDate and endDate" in {
    Test.outputFindings();
    Test.summaryOutput();
    Test.getGamesBetween(LocalDateTime.parse("2011-01-01T00:00:00"), LocalDateTime.parse("2021-12-31T23:59:59"));
  }

  "getGameReviews(Long, String)" should "go out and fetch missing reviews for the given game_id" in {
    Test.outputFindings();
    Test.getGameReviews(1L, LocalDateTime.now().getYear.toString);
  }

  it should "error and do nothing if the game does not exist in our games datastore" in {
    Test.outputFindings();
    Test.getGameReviews(-1, LocalDateTime.now().getYear.toString);
  }

  "getGameArticles(Long, String)" should "go out and fetch missing articles for the given game_id and partition year" in {
    Test.outputFindings();
    Test.getGameArticles(1L, LocalDateTime.now().getYear.toString);
  }

  it should "error and do nothing if the game does not exist in our games datastore" in {
    Test.outputFindings();
    Test.getGameArticles(-1, LocalDateTime.now().getYear.toString);
  }

  "getAllReviews()" should "go out and fetch all missing reviews from Gamespot's API" in {
    Test.outputFindings();
    Test.getAllReviews();
  }

  "getAllGameThemes()" should "go out and fetch all missing game themes from Gamespot's API" in {
    Test.outputFindings();
    Test.getAllGameThemes();
  }

  "getAllGameGenres()" should "go out and fetch all missing game genres from Gamespot's API" in {
    Test.outputFindings();
    Test.getAllGameGenres();
  }
}