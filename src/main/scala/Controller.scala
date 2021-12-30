import scala.io.StdIn.{readInt, readLine}

object Controller {
  private var state : String = "";

  def intro() : Unit = {
    val menuOptions : List[Int] = List(1, 2, 3);
    var menuChoice : Int = 0;
    state = "Intro";

    while (state.equals("Intro")) {
      println();
      View.welcome();
      do {
        print("Please select a menu option: ");
        try {
          menuChoice = readInt();
          if (!menuOptions.contains(menuChoice))
            println("That is not a valid menu option! Please try again!");
        } catch {
          case nfe : NumberFormatException => {
            println("That is not a valid number! Please try again!");
          }
        }
      } while (!menuOptions.contains(menuChoice));

      menuChoice match {
        case 1 => login();
        case 2 => registerAccount();
        case 3 => state = "Exit";
      }

      menuChoice = 0;
    }
  }

  private def login() : Unit = {
    var username : String = "";
    var password : String = "";
    state = "Login";

    while (state.equals("Login")) {
      println();
      print("Please enter your username (or nothing to return to the previous menu): ");
      username = readLine();
      if (username.isEmpty)
        state = "Intro";
      else {
        print("Please enter your password (or nothing to return to the previous menu): ");
        password = readLine();
        if (password.isEmpty)
          state = "Intro";
        else if (HiveDBManager.authenticate(username, password, false) > 0) {
          BasicController.authorize(username, password)
          state = "Intro";
        };
        else if (HiveDBManager.authenticate(username, password, true) > 0) {
          null; //AdminController.authorize(username, password);
          state = "Intro";
        }
        else
          println("Credentials do not match an account in this system. Please try again.");
      }
    }
  }

  private def registerAccount() : Unit = {
    var username : String = "";
    var password : String = "";
    state = "Register";

    while (state.equals("Register")) {
      println();
      do {
        print("Please enter your desired username (or nothing to return to the previous menu: ");
        username = readLine();
        if (HiveDBManager.usernameExists(username))
          println("That username is already taken, please choose another one!");
      } while (HiveDBManager.usernameExists(username));

      if (username.isEmpty)
        state = "Intro";
      else {
        do {
          print("Please enter your desired password: ");
          password = readLine();
          if (password.isEmpty)
            println("Your password is empty! Passwords must be non-empty. Please try again.");
        } while (password.isEmpty);

        HiveDBManager.addUser(username, password);
        println(s"User account $username created!");
        state = "Intro";
      }
    }
  }
}