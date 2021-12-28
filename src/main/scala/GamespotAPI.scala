import scalaj.http.{Http, HttpOptions, HttpRequest, HttpResponse}

import java.io.IOException
import scala.io.BufferedSource

class GamespotAPI {
  private final val URL : String = "https://www.gamespot.com/api/";
  private var apiKey : String = "";
  private var request : HttpRequest = null;
  private var games : Boolean = false;
  private var articles : Boolean = false;
  private var reviews : Boolean = false;
  protected val headerFields : List[String] = List(
    "error", "limit", "offset", "number_of_page_results", "number_of_total_results", "status_code", "results"
  );
  protected val articleFields : List[String] = List(
    "id", "authors", "title", "deck", "lede", "body", "image", "categories", "associations", "publish_date", "update_date", "videos_api_url", "site_detail_url"
  );
  protected val articleFilterFields : List[String] = List(
    "id", "title", "categories", "associations", "publish_date", "association"
  );
  protected val articleSortFields : List[String] = List(
    "id", "title", "publish_date"
  );
  protected val gameFields : List[String] = List(
    "release_date", "deck", "description", "id", "name", "image", "genres", "themes", "franchises", "site_detail_url", "videos_api_url", "articles_api_url", "reviews_api_url", "images_api_url", "releases_api_url"
  );
  protected val gameFilterFields : List[String] = List("release_date", "id", "name", "genres", "themes", "franchises");
  protected val gameSortFields : List[String] = List("release_date", "id", "name");
  protected val reviewFields : List[String] = List(
    "id", "authors", "title", "deck", "lede", "body", "good", "bad", "image", "publish_date", "update_date", "score", "review_type", "movie", "television" ,"game", "releases", "site_detail_url", "videos_api_url"
  );
  protected val reviewFilterFields : List[String] = List("id", "title", "publish_date", "update_date", "association");
  protected val reviewSortFields : List[String] = List("id", "title", "publish_date", "update_date", "score");

  protected def setAPIKey(newKey : String) : Unit = {
    apiKey = newKey;
  }

  protected def getAPIKey() : String = {
    return apiKey;
  }

  protected def getRequest() : HttpRequest = {
    return request;
  }

  protected def getURL() : String = {
    return request.url;
  }

  protected def setURL(newURL : String, games : Boolean, articles : Boolean, reviews : Boolean) : Unit = {
    request = Http(newURL).copy(headers = Seq(("User-Agent", "ryanmgrum")));
    this.games = games;
    this.articles = articles;
    this.reviews = reviews;
  }

  protected def reset() : Unit = {
    apiKey = "";
    request = null;
    games = false;
    articles = false;
    reviews = false;
  }

  protected def init() : Unit = {
    request = Http(URL).copy(headers = Seq(("User-Agent", "ryanmgrum")));
    games = false;
    articles = false;
    reviews = false;
  }

  protected def selectEndpoint(games : Boolean, articles : Boolean, reviews : Boolean) : Unit = {
    if (games) {
      this.games = true;
      this.articles = false;
      this.reviews = false;
      request = Http(URL + "games/").copy(headers = Seq(("User-Agent", "ryanmgrum")));
    } else if (articles) {
      this.games = false;
      this.articles = true;
      this.reviews = false;
      request = Http(URL + "articles/").copy(headers = Seq(("User-Agent", "ryanmgrum")));
    } else if (reviews) {
      this.games = false;
      this.articles = false;
      this.reviews = true;
      request = Http(URL + "reviews/").copy(headers = Seq(("User-Agent", "ryanmgrum")));
    }
  }

  protected def setFormat(xml : Boolean, json : Boolean, jsonp : Boolean) : Unit = {
    if (xml)
      request = request.params(("format", "xml"));
    else if (json)
      request = request.params(("format", "json"));
    else if (jsonp)
      request = request.params(("format", "jsonp"));
  }

  protected def setOffset(num : Long) : Unit = {
    if (num >= 0)
      request = request.params(("offset", num.toString));
  }

  protected def setLimit(num : Int) : Unit = {
    if (num > 0 && num <= 100)
      request = request.params(("limit", num.toString));
  }

  protected def filterField(fieldName : String, separator : String, value : String) : Unit = {
    if (
      (articles && articleFilterFields.contains(fieldName)) ||
      (games && gameFilterFields.contains(fieldName)) ||
      (reviews && reviewFilterFields.contains(fieldName))
    )
      request = request.params(("filter", fieldName + separator + value));
  }

  protected def filterField(fieldName : String, separator : String, value : Int) : Unit = {
    if (
      (articles && articleFilterFields.contains(fieldName)) ||
      (games && gameFilterFields.contains(fieldName)) ||
      (reviews && reviewFilterFields.contains(fieldName))
    )
      request = request.params(("filter", fieldName + separator + value.toString));
  }

  protected def sortField(fieldName : String, asc : Boolean) : Unit = {
    if (
      (articles && articleFilterFields.contains(fieldName)) ||
      (games && gameFilterFields.contains(fieldName)) ||
      (reviews && reviewFilterFields.contains(fieldName))
    )
      if (asc)
        request = request.params(("sort", fieldName + ":asc"))
      else
        request = request.params(("sort", fieldName + ":desc"))
  }

  protected def getResults() : String = {
    var result:String = "";
    try {
      val response : HttpResponse[String] = request.params(("api_key", apiKey)).option(HttpOptions.connTimeout(10000)).option(HttpOptions.readTimeout(50000)).asString
      result = response.body;
    } catch {
      case i : IOException => {
        i.printStackTrace();
      }
    }
    return result;
  }

  protected def getAPIConfig(filename : String) : Map[String, String] = {
    val file : BufferedSource = scala.io.Source.fromURL(getClass.getResource("/" + filename));
    var result : Map[String, String] = Map();
    for (line : String <- file.getLines)
      result += (line.split("=")(0) -> line.split("=")(1));
    file.close();
    return result;
  }
}