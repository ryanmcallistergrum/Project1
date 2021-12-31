object Main {
  def main(args : Array[String]) : Unit = {
    HiveDBManager.startupDB();
    Controller.intro();
  }
}