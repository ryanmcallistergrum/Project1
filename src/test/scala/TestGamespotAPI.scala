import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should
import scalaj.http.HttpRequest

class TestGamespotAPI extends AnyFlatSpec with should.Matchers {
  object Test extends GamespotAPI {
    final val API_CONFIG_FILENAME : String = "apiDetails.txt";
    def getHeaderFields() : List[String] = headerFields;
    def getArticleFields() : List[String] = articleFields;
    def getArticleFilterFields() : List[String] = articleFilterFields;
    def getArticleSortFields() : List[String] = articleSortFields;
    def getReviewFields() : List[String] = reviewFields;
    def getReviewFilterFields() : List[String] = reviewFilterFields;
    def getReviewSortFields() : List[String] = reviewSortFields;
    def getGameFields() : List[String] = gameFields;
    def getGameFilterFields() : List[String] = gameFilterFields;
    def getGameSortFields() : List[String] = gameSortFields;
    override def setAPIKey(key : String) : Unit = super.setAPIKey(key);
    override def getAPIKey() : String = super.getAPIKey();
    override def getRequest() : HttpRequest = super.getRequest();
    override def getURL() : String = super.getURL();
    override def setURL(newURL : String, games : Boolean, articles : Boolean, reviews : Boolean) : Unit = super.setURL(newURL, games, articles, reviews);
    override def reset() : Unit = super.reset();
    override def init() : Unit = super.init();
    override def selectEndpoint(games : Boolean, articles : Boolean, reviews : Boolean) : Unit = super.selectEndpoint(games, articles, reviews);
    override def setFormat(xml : Boolean, json : Boolean, jsonp : Boolean) : Unit = super.setFormat(xml, json, jsonp);
    override def setOffset(offset : Long) : Unit = super.setOffset(offset);
    override def setLimit(limit : Int) : Unit = super.setLimit(limit);
    override def fieldList(fields: List[String]): Unit = super.fieldList(fields);
    override def filterField(fieldName : String, separator : String, value : String) : Unit = super.filterField(fieldName, separator, value);
    override def filterField(fieldName : String, separator : String, value : Int) : Unit = super.filterField(fieldName, separator, value);
    override def sortField(fieldName : String, asc : Boolean) : Unit = super.sortField(fieldName, asc);
    override def getResults() : String = super.getResults();
    override def getAPIConfig(filename : String) : Map[String, String] = super.getAPIConfig(filename);
  }

  "setAPIKey(String)" should "modify the API key stored in the apiKey attribute" in {
    assert(Test.getAPIKey().isEmpty);
    Test.setAPIKey("test");
    assert(Test.getAPIKey().equals("test"));
  }

  "getAPIKey()" should "return the String stored in the apiKey attribute" in {
    Test.setAPIKey("test");
    assert(Test.getAPIKey().equals("test"));
  }

  "getRequest()" should "initially be null" in {
    assert(Test.getRequest() == null);
  }

  "getURL()" should "return a non-empty String containing Gamespot's API" in {
    assert(Test.getURL().equals("https://www.gamespot.com/api/"));
  }

  "setURL(String, Boolean, Boolean, Boolean)" should "set the URL for the request and the endpoint Boolean" in {
    Test.setURL("test", false, false, false);
    assert(Test.getURL().equals("test"));
  }

  "reset()" should "set request to null and apiKey to an empty String" in {
    Test.reset();
    assert(Test.getRequest == null && Test.getAPIKey().isEmpty);
  }

  "init()" should "set request to just the Gamespot API URL" in {
    Test.init();
    assert(Test.getRequest().url.equals(Test.getURL()));
  }

  "selectEndpoint(Boolean, Boolean, Boolean)" should "append either 'games/', 'articles/' or 'reviews/' to the end of the URL after calling it" in {
    Test.selectEndpoint(true, false, false);
    assert(Test.getRequest().url.equals(Test.getURL() + "games/"));
    Test.selectEndpoint(false, true, false);
    assert(Test.getRequest().url.equals(Test.getURL() + "articles/"));
    Test.selectEndpoint(false, false, true);
    assert(Test.getRequest().url.equals(Test.getURL() + "reviews/"));
  }

  it should "take the first true value and ignore the rest in order of the parameters" in {
    Test.selectEndpoint(true, true, false);
    assert(Test.getRequest().url.equals(Test.getURL() + "games/"));
    Test.selectEndpoint(true, false, true);
    assert(Test.getRequest().url.equals(Test.getURL() + "games/"));
    Test.selectEndpoint(true, true, true);
    assert(Test.getRequest().url.equals(Test.getURL() + "games/"));
    Test.selectEndpoint(false, true, true);
    assert(Test.getRequest().url.equals(Test.getURL() + "articles/"));
  }

  it should "not modify the URL at all if all the parameters are false" in {
    Test.init();
    Test.selectEndpoint(false, false, false);
    assert(Test.getRequest().url.equals(Test.getURL()));
  }

  "setFormat(Boolean, Boolean, Boolean)" should "be run after executing init()" in {
    Test.reset();
    a [NullPointerException] should be thrownBy {
      Test.setFormat(true, false, false);
    }
    a [NullPointerException] should be thrownBy {
      Test.setFormat(false, true, false);
    }
    a [NullPointerException] should be thrownBy {
      Test.setFormat(false, false, true);
    }
    a [NullPointerException] should be thrownBy {
      Test.setFormat(true, true, false);
    }
    a [NullPointerException] should be thrownBy {
      Test.setFormat(true, false, true);
    }
    a [NullPointerException] should be thrownBy {
      Test.setFormat(false, true, true);
    }
    a [NullPointerException] should be thrownBy {
      Test.setFormat(true, true, true);
    }

    Test.init();
    noException should be thrownBy {
      Test.setFormat(true, false, false);
    }
    assert(Test.getRequest().params.nonEmpty);
  }

  it should "add the appropriate format param depending on whether the user chooses xml, json, or jsonp" in {
    Test.init();
    Test.setFormat(true, false, false);
    assert(Test.getRequest().params.contains(("format", "xml")));

    Test.init();
    Test.setFormat(false, true, false);
    assert(Test.getRequest().params.contains(("format", "json")));

    Test.init();
    Test.setFormat(false, false, true);
    assert(Test.getRequest().params.contains(("format", "jsonp")));
  }

  it should "take the first true value and ignore the rest in order of the parameters" in {
    Test.init();
    Test.setFormat(true, true, false);
    assert(Test.getRequest().params.contains(("format", "xml")));
    Test.init();
    Test.setFormat(true, false, true);
    assert(Test.getRequest().params.contains(("format", "xml")));
    Test.init();
    Test.setFormat(true, true, true);
    assert(Test.getRequest().params.contains(("format", "xml")));

    Test.init();
    Test.setFormat(false, true, true);
    assert(Test.getRequest().params.contains(("format", "json")));
  }

  it should "not add a param if all the parameters are false" in {
    Test.init();
    Test.setFormat(false, false, false);
    assert(Test.getRequest().params.isEmpty);
  }

  "setOffset(Long)" should "add a param containing the result to start from" in {
    Test.init();
    Test.setOffset(1L);
    assert(Test.getRequest().params.contains(("offset", "1")));
  }

  it should "not add a param if the offset is below zero" in {
    Test.init();
    Test.setOffset(-1L);
    assert(Test.getRequest().params.isEmpty);
  }

  it should "add a param if greater than or equal to zero" in {
    Test.init();
    Test.setOffset(0L);
    assert(Test.getRequest().params.contains(("offset", "0")));

    Test.init();
    Test.setOffset(1L);
    assert(Test.getRequest().params.contains(("offset", "1")));
  }

  "setLimit(Int)" should "add a param containing the number of results to return" in {
    Test.init();
    Test.setLimit(1);
    assert(Test.getRequest().params.contains(("limit", "1")));
  }

  it should "not add a param if the value is below 1 or above 100" in {
    Test.init();
    Test.setLimit(0);
    assert(Test.getRequest().params.isEmpty);
    Test.init();
    Test.setLimit(101);
    assert(Test.getRequest().params.isEmpty);
  }

  "fieldList(List[String])" should "add a param containing the list of fields to return" in {
    val list : List[String] = List("genres", "videos_api_url");
    Test.init();
    Test.selectEndpoint(true, false, false);
    Test.fieldList(list);
    assert(Test.getRequest().params.contains(("field_list", list.reduce(_ + "," + _))));
  }

  "filterField(String, String)" should "add a param containing the fieldName and value" in {
    Test.init();
    Test.selectEndpoint(true, false, false);
    Test.filterField(Test.getGameFilterFields().head, ":", "test");
    assert(Test.getRequest().params.contains(("filter", Test.getGameFilterFields().head + ":test")));

    Test.init();
    Test.selectEndpoint(false, true, false);
    Test.filterField(Test.getArticleFilterFields().head, ":", "test");
    assert(Test.getRequest().params.contains(("filter", Test.getArticleFilterFields().head + ":test")));

    Test.init();
    Test.selectEndpoint(false, false, true);
    Test.filterField(Test.getReviewFilterFields().head, ":", "test");
    assert(Test.getRequest().params.contains(("filter", Test.getReviewFilterFields().head +  ":test")));
  }

  it should "add the first true parameter in the parameter list and ignore the rest" in {
    Test.init();
    Test.selectEndpoint(true, true, false);
    Test.filterField(Test.getGameFilterFields().head, ":", "test");
    assert(Test.getRequest().params.contains(("filter", Test.getGameFilterFields().head + ":test")));
    Test.init();
    Test.selectEndpoint(true, false, true);
    Test.filterField(Test.getGameFilterFields().head, ":", "test");
    assert(Test.getRequest().params.contains(("filter", Test.getGameFilterFields().head + ":test")));

    Test.init();
    Test.selectEndpoint(false, true, true);
    Test.filterField(Test.getArticleFilterFields().head, ":", "test");
    assert(Test.getRequest().params.contains(("filter", Test.getArticleFilterFields().head + ":test")));
  }

  "filterField(String, Int)" should "add a param containing the fieldName and value" in {
    Test.init();
    Test.selectEndpoint(true, false, false);
    Test.filterField(Test.getGameFilterFields().head, ":", 1);
    assert(Test.getRequest().params.contains(("filter", Test.getGameFilterFields().head + ":" + 1)));

    Test.init();
    Test.selectEndpoint(false, true, false);
    Test.filterField(Test.getArticleFilterFields().head, ":", 1);
    assert(Test.getRequest().params.contains(("filter", Test.getArticleFilterFields().head + ":" + 1)));

    Test.init();
    Test.selectEndpoint(false, false, true);
    Test.filterField(Test.getReviewFilterFields().head, ":", 1);
    assert(Test.getRequest().params.contains(("filter", Test.getReviewFilterFields().head +  ":" + 1)));
  }

  it should "add the first true parameter in the parameter list and ignore the rest" in {
    Test.init();
    Test.selectEndpoint(true, true, false);
    Test.filterField(Test.getGameFilterFields().head, ":", 1);
    assert(Test.getRequest().params.contains(("filter", Test.getGameFilterFields().head + ":" + 1)));
    Test.init();
    Test.selectEndpoint(true, false, true);
    Test.filterField(Test.getGameFilterFields().head, ":", 1);
    assert(Test.getRequest().params.contains(("filter", Test.getGameFilterFields().head + ":" + 1)));

    Test.init();
    Test.selectEndpoint(false, true, true);
    Test.filterField(Test.getArticleFilterFields().head, ":", 1);
    assert(Test.getRequest().params.contains(("filter", Test.getArticleFilterFields().head + ":" + 1)));
  }

  "sortField(String, Boolean)" should "add a param containing the fieldName and direction to sort" in {
    Test.init();
    Test.selectEndpoint(true, false, false);
    Test.sortField(Test.getGameSortFields().head, true);
    assert(Test.getRequest().params.contains(("sort", Test.getGameSortFields().head + ":asc")));
    Test.init();
    Test.selectEndpoint(true, false, false);
    Test.sortField(Test.getGameSortFields().head, false);
    assert(Test.getRequest().params.contains(("sort", Test.getGameSortFields().head + ":desc")));

    Test.init();
    Test.selectEndpoint(false, true, false);
    Test.sortField(Test.getArticleSortFields().head, true);
    assert(Test.getRequest().params.contains(("sort", Test.getArticleSortFields().head + ":asc")));
    Test.init();
    Test.selectEndpoint(false, true, false);
    Test.sortField(Test.getArticleSortFields().head, false);
    assert(Test.getRequest().params.contains(("sort", Test.getArticleSortFields().head + ":desc")));

    Test.init();
    Test.selectEndpoint(false, false, true);
    Test.sortField(Test.getReviewSortFields().head, true);
    assert(Test.getRequest().params.contains(("sort", Test.getReviewSortFields().head + ":asc")));
    Test.init();
    Test.selectEndpoint(false, false, true);
    Test.sortField(Test.getReviewSortFields().head, false);
    assert(Test.getRequest().params.contains(("sort", Test.getReviewSortFields().head + ":desc")));
  }

  it should "add the first true parameter in the parameter list and ignore the rest" in {
    Test.init();
    Test.selectEndpoint(true, true, false);
    Test.sortField(Test.getGameSortFields().head, true);
    assert(Test.getRequest().params.contains(("sort", Test.getGameSortFields().head + ":asc")));
    Test.init();
    Test.selectEndpoint(true, true, false);
    Test.sortField(Test.getGameSortFields().head, false);
    assert(Test.getRequest().params.contains(("sort", Test.getGameSortFields().head + ":desc")));
    Test.init();
    Test.selectEndpoint(true, false, true);
    Test.sortField(Test.getGameSortFields().head, true);
    assert(Test.getRequest().params.contains(("sort", Test.getGameSortFields().head + ":asc")));
    Test.init();
    Test.selectEndpoint(true, false, true);
    Test.sortField(Test.getGameSortFields().head, false);
    assert(Test.getRequest().params.contains(("sort", Test.getGameSortFields().head + ":desc")));

    Test.init();
    Test.selectEndpoint(false, true, true);
    Test.sortField(Test.getArticleSortFields().head, true);
    assert(Test.getRequest().params.contains(("sort", Test.getArticleSortFields().head + ":asc")));
    Test.init();
    Test.selectEndpoint(false, true, true);
    Test.sortField(Test.getArticleSortFields().head, false);
    assert(Test.getRequest().params.contains(("sort", Test.getArticleSortFields().head + ":desc")));
  }

  "getResults()" should "return a String containing game, article, or review info" in {
    Thread.sleep(5000);
    Test.init();
    Test.setAPIKey(Test.getAPIConfig(Test.API_CONFIG_FILENAME)("apiKey"));
    Test.selectEndpoint(true, false, false);
    Test.setLimit(1);
    var results : String = Test.getResults();
    assert(results.nonEmpty);

    Thread.sleep(5000);
    Test.init();
    Test.setAPIKey(Test.getAPIConfig(Test.API_CONFIG_FILENAME)("apiKey"));
    Test.selectEndpoint(false, true, false);
    Test.setLimit(1);
    results = Test.getResults();
    assert(results.nonEmpty);

    Thread.sleep(5000);
    Test.init();
    Test.setAPIKey(Test.getAPIConfig(Test.API_CONFIG_FILENAME)("apiKey"));
    Test.selectEndpoint(false, false, true);
    Test.setLimit(1);
    results = Test.getResults();
    assert(results.nonEmpty);
  }

  it should "return Gamespot's main page if no endpoint is selected" in {
    Thread.sleep(5000);
    Test.init();
    Test.setAPIKey(Test.getAPIConfig(Test.API_CONFIG_FILENAME)("apiKey"));
    var results : String = Test.getResults();
    assert(results.contains("<!doctype html>"));
  }

  it should "return the XML format by default if no format is chosen" in {
    Thread.sleep(5000);
    Test.init();
    Test.setAPIKey(Test.getAPIConfig(Test.API_CONFIG_FILENAME)("apiKey"));
    Test.selectEndpoint(true, false, false);
    Test.setLimit(1);
    var results : String = Test.getResults();
    assert(results.contains("<?xml version=\"1.0\" encoding=\"utf-8\"?>"));

    Thread.sleep(5000);
    Test.init();
    Test.setAPIKey(Test.getAPIConfig(Test.API_CONFIG_FILENAME)("apiKey"));
    Test.selectEndpoint(false, true, false);
    Test.setLimit(1);
    results = Test.getResults();
    assert(results.contains("<?xml version=\"1.0\" encoding=\"utf-8\"?>"));

    Thread.sleep(5000);
    Test.init();
    Test.setAPIKey(Test.getAPIConfig(Test.API_CONFIG_FILENAME)("apiKey"));
    Test.selectEndpoint(false, false, true);
    Test.setLimit(1);
    results = Test.getResults();
    assert(results.contains("<?xml version=\"1.0\" encoding=\"utf-8\"?>"));
  }

  // No need to hammer Gamespot's API endpoint with pointless tests.
  /*it should "return results after filtering on each available field for each endpoint" in {
    for(field : String <- Test.getGameFilterFields()) {
      Thread.sleep(5000);
      Test.init();
      Test.setAPIKey(Test.getAPIConfig(Test.API_CONFIG_FILENAME)("apiKey"));
      Test.selectEndpoint(true, false, false);
      Test.setLimit(1);
      Test.filterField(field, "test");
      var results : String = Test.getResults();
      assert(results.nonEmpty);
    }
  }*/

  "getAPIConfig(String)" should "return a Map[String, String] containing sensitive details for connecting to the Gamespot API" in {
    var map: Map[String, String] = Map();
    noException should be thrownBy {
      map = Test.getAPIConfig(Test.API_CONFIG_FILENAME);;
    }
    assert(map.nonEmpty && map.contains("apiKey"));
  }

  it should "throw a NullPointerException when it cannot find the file" in {
    var map: Map[String, String] = Map();
    a [NullPointerException] should be thrownBy {
      map = Test.getAPIConfig("fakeFile");;
    }
    assert(map.isEmpty);
  }
}