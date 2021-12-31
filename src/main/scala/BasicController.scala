import scala.io.StdIn.{readInt, readLine}

object BasicController {
  private var user_id : Int = 0;
  private var state : String = "";

  def authorize(username : String, password : String) : Unit = {
    user_id = HiveDBManager.authenticate(username, password, false);
    if (user_id > 0) {
      println("Login successful, entering Home Screen...");
      homeScreen()
    }
  }

  private def homeScreen() : Unit = {
    state = "Home Screen";
    while (state.equals("Home Screen")) {
      println();

      val inputThread : Thread = new Thread() {
        val updateChecker : APIFetcher = new APIFetcher();
        val backgroundThread : Thread = new Thread() {
          override def run(): Unit = {
            updateChecker.outputFindings();
            updateChecker.summaryOutput();
            updateChecker.getLatest();
          }
        }
        override def run() : Unit = {
          println("Press enter at any time to go to the User Menu.");
          backgroundThread.start();
          readLine();
          updateChecker.stop();
        }
      }

      inputThread.start();
      inputThread.join();
      userMenu();
    }
  }

  private def userMenu() : Unit = {
    state = "User Menu";
    var userInput : Int = 0;
    val menuOptions : List[Int] = List(1, 2, 3, 4, 5);

    while (state.equals("User Menu")) {
      println();
      BasicView.userMenu();
      do {
        print("Please select a menu option: ");
        try {
          userInput = readInt();
          if (!menuOptions.contains(userInput))
            println("Invalid menu selection! Please try again.");
        } catch {
          case nfe : NumberFormatException => {
            println("That is not a valid number! Please try again.");
          }
        }
      } while (!menuOptions.contains(userInput));

      userInput match {
        case 1 => state = "Home Screen";
        case 2 => executeQuery();
        case 3 => changeUsername();
        case 4 => changePassword();
        case 5 => state = "Intro";
      }

      userInput = 0;
    }
  }

  private def executeQuery() : Unit = {
    state = "Execute Query";

    while (state.equals("Execute Query")) {
      println();
      val queries: Map[Int, String] = HiveDBManager.getQueries();


      if (queries.isEmpty) {
        println("No saved queries found! Please notify an administrator if you are expecting queries. Returning to the User Menu.");
        print("Press enter to continue...")
        readLine();
        state = "User Menu";
      } else {
        var userInput: Int = -1;
        for (query : Int <- queries.keys.toList.sorted)
          println(s"$query. ${queries(query)}");
        do {
          print("Please select a query to execute, or enter 0 to return to the User Menu: ");
          try {
            userInput = readInt();
            if (!queries.keySet.contains(userInput) && userInput != 0)
              println("Invalid query identifier! Please try again.");
          } catch {
            case nfe : NumberFormatException => {
              println("Invalid number entered! Please try again.");
            }
          }
        } while (userInput == -1);

        if (userInput == 0)
          state = "User Menu";
        else if (!HiveDBManager.queryExists(userInput)) {
          println("Query deleted between selection and execution! Please select another query or notify an administrator.")
          print("Press enter when ready to continue...");
          readLine();
        } else {
          HiveDBManager.showQuery(userInput);
          print("Press enter when ready to continue...");
          println();
          val choice : String = readLine("Would you like to save the results of the query? Enter 'y' to continue or anything else to skip: ");
          if (choice.toLowerCase().equals("y")) {
            val filePath : String = readLine("Please enter the either the local filename or the full file path (enter nothing to skip): ");
            if (filePath.nonEmpty)
              HiveDBManager.exportQueryResults(userInput, filePath);
          }
        }

        userInput = -1;
      }
    }
  }

  private def changeUsername() : Unit = {
    state = "Change Username";
    while (state.equals("Change Username")) {
      var userInput: String = "";
      do {
        println();
        print("Please enter your new username, or enter nothing to return to the User Menu: ");
        userInput = readLine();
        if (HiveDBManager.usernameExists(userInput))
          println("Username $userInput already exists please choose another username.");
      } while (HiveDBManager.usernameExists(userInput));

      if (userInput.nonEmpty)
        if (!HiveDBManager.usernameExists(userInput)) {
          HiveDBManager.updateUsername(user_id, userInput)
          println(s"Username changed to $userInput!");
          print("Press enter to continue...");
          readLine();
          state = "User Menu";
        } else {
          println("Another user either created an account or changed their username to your selection between selection and updating. Please choose another username.")
          print("Press enter to continue...");
          readLine();
        }
      else
        state = "User Menu";
    }
  }

  private def changePassword() : Unit = {
    state = "Change Password";
    while (state.equals("Change Password")) {
      var oldPassword : String = "";
      var newPassword : String = "";

      print("For security, please enter your old password (or nothing to return to the User Menu): ");
      oldPassword = readLine();
      if (oldPassword.nonEmpty) {

        print("Please enter your new password (or nothing to return to the User Menu): ");
        newPassword = readLine();
        if (newPassword.nonEmpty)
          if (HiveDBManager.updatePassword(user_id, oldPassword, newPassword)) {
            print("Password successfully changed! Press enter to continue...");
            readLine();
            state = "User Menu";
          } else {
            println("Old password does not match what is in the system. Please try again.");
            print("Press enter to continue...");
            readLine();
          }
        else
          state = "User Menu";
      } else
        state = "User Menu";
    }
  }
}