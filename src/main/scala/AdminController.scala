import org.apache.spark.sql.AnalysisException

import scala.io.StdIn.{readInt, readLine};

object AdminController extends HiveDBManager {
  private var user_id : Int = 0;
  private var state : String = "";

  def authorize(username : String, password : String) : Unit = {
    user_id = HiveDBManager.authenticate(username, password, true);
    if (user_id > 0) {
      println("Login successful, entering Home Screen...");
      homeScreen();
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
    val menuOptions : List[Int] = List(1, 2, 3, 4, 5, 6);

    while (state.equals("User Menu")) {
      println();
      AdminView.userMenu();
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
        case 2 => manageQueries();
        case 3 => manageUsers();
        case 4 => changeUsername();
        case 5 => changePassword();
        case 6 => state = "Intro";
      }

      userInput = 0;
    }
  }

  private def manageQueries() : Unit = {
    state = "Manage Queries";
    var userInput : Int = -1;
    val menuOptions : List[Int] = List(1, 2, 3, 4);

    while (state.equals("Manage Queries")) {
      println();
      AdminView.manageQueries();
      do {
        print("Please select a menu option, or enter 0 to return to the User Menu: ");
        try {
          userInput = readInt();
          if (!menuOptions.contains(userInput) && userInput != 0)
            println("Invalid menu selection! Please try again.");
        } catch {
          case nfe : NumberFormatException => {
            println("That is not a valid number! Please try again.");
          }
        }
      } while (!menuOptions.contains(userInput) && userInput != 0);

      userInput match {
        case 1 => executeQuery();
        case 2 => renameQuery();
        case 3 => writeQuery();
        case 4 => deleteQuery();
        case 0 => state = "User Menu";
      }

      userInput = -1;
    }
  }

  private def executeQuery() : Unit = {
    state = "Execute Query";

    while (state.equals("Execute Query")) {
      println();
      val queries: Map[Int, String] = HiveDBManager.getQueries();


      if (queries.isEmpty) {
        println("No saved queries found! Returning to the Query Management Menu.");
        print("Press enter to continue...")
        readLine();
        state = "Manage Queries";
      } else {
        var userInput: Int = -1;
        for (query : Int <- queries.keys.toList.sorted)
          println(s"$query. ${queries(query)}");
        do {
          print("Please select a query to execute, or enter 0 to return to the Query Management Menu: ");
          try {
            userInput = readInt();
            if (!queries.keySet.contains(userInput) && userInput != 0)
              println("Invalid query identifier! Please try again.");
          } catch {
            case nfe : NumberFormatException => {
              println("Invalid number entered! Please try again.");
            }
          }
        } while (!queries.keySet.contains(userInput) && userInput != 0);

        if (userInput == 0)
          state = "Manage Queries";
        else if (!HiveDBManager.queryExists(userInput)) {
          println("Query deleted between selection and execution! Please select another query.")
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

  private def renameQuery() : Unit = {
    state = "Rename Query";

    while (state.equals("Rename Query")) {
      println();
      val queries: Map[Int, String] = getQueries();
      if (queries.isEmpty) {
        println("No saved queries found! Returning to the Query Management Menu.");
        print("Press enter to continue...")
        readLine();
        state = "Manage Queries";
      } else {
        var userInput: Int = -1;
        for (query: Int <- queries.keys.toList.sorted)
          println(s"$query. ${queries(query)}");
        do {
          print("Please select a query to rename, or enter 0 to return to the Query Management Menu: ");
          try {
            userInput = readInt();
            if (!queries.keys.toList.contains(userInput) && userInput != 0)
              println("Invalid query identifier! Please try again.");
          } catch {
            case nfe: NumberFormatException => {
              println("Invalid number entered! Please try again.");
            }
          }
        } while (!queries.keys.toList.contains(userInput) && userInput != 0);

        if (userInput == 0)
          state = "Manage Queries";
        else {
          var newName : String = "";
          do {
            print("Please enter in the new name for the query, or enter nothing to return to the query list: ");
            newName = readLine();
            if (queryNameExists(newName))
              println(s"Query name $newName already exists! Please choose another name.");
          } while (queryNameExists(newName))

          if (newName.nonEmpty) {
            renameQuery(userInput, newName)
            if (getQueries().values.exists(q => q.equals(newName)))
              println(s"Query renamed to $newName!");
            else
              println("Something went wrong during renaming! Please try again.");
            print("Press enter to continue...");
            readLine();
          }
        }
      }
    }
  }

  private def writeQuery() : Unit = {
    state = "Write Query";
    while (state.equals("Write Query")) {
      var query : String = "";
      println("Copy-and-paste (recommended) or type in your query and, when done, end it with a semicolon; for single-quotes, be sure to escape them with two backslashes (\\\\).);");
      println("To return to the Query Management Menu, enter nothing.");
      do {
        query += readLine();
      } while (!query.contains(";"));
      if (query.replaceAll(";", "").nonEmpty) {
        try {
          showQuery(connect(), query.replaceAll(";", ""));
        } catch {
          case ae : AnalysisException => {
            ae.printStackTrace();
          }
        }
        println();
        println("If the results are as you expect, you can choose to save your query now.");
        print("If you want to save your query, enter 'y', otherwise enter anything else to skip: ");
        val saveChoice : String = readLine();
        if (saveChoice.equals("y"))
          saveQuery(query);
      } else
        state = "Manage Queries";
    }
  }

  private def saveQuery(query : String) : Unit = {
    var name : String = "";
    do {
      name = readLine("Enter in the name to use for your query, or enter nothing to return to writing queries: ");
      if (queryNameExists(name))
        println(s"Query name $name already exists, please enter in another name.");
    } while (queryNameExists(name))

    if (!queryNameExists(name) && name.nonEmpty) {
      saveQuery(user_id, name, query);
      if (queryNameExists(name))
        println(s"Query ${"\""}$name${"\""} saved successfully!");
    }
  }

  private def deleteQuery() : Unit = {
    state = "Delete Query";
    while (state.equals("Delete Query")) {
      println();
      val queries: Map[Int, String] = getQueries();
      if (queries.isEmpty) {
        println("No saved queries found! Returning to the Query Management Menu.");
        print("Press enter to continue...")
        readLine();
        state = "Manage Queries";
      } else {
        var userInput: Int = -1;
        for (query: Int <- queries.keys.toList.sorted)
          println(s"$query. ${queries(query)}");
        do {
          print("Please select a query to delete, or enter 0 to return to the Query Management Menu: ");
          try {
            userInput = readInt();
            if (!queries.keys.toList.contains(userInput) && userInput != 0)
              println("Invalid query identifier! Please try again.");
          } catch {
            case nfe: NumberFormatException => {
              println("Invalid number entered! Please try again.");
            }
          }
        } while (!queries.keys.toList.contains(userInput) && userInput != 0);

        if (userInput == 0)
          state = "Manage Queries";
        else {
          deleteQuery(userInput)
          if (!getQueries().keys.toList.contains(userInput))
            println(s"Query ${"\""}${queries(userInput)}${"\""} deleted successfully!");
          else
            println("Something went wrong during deletion! Please try again.");
          print("Press enter to continue...");
          readLine();
        }
      }
    }
  }

  private def manageUsers() : Unit = {
    state = "Manage Users";
    var userInput : Int = 0;
    val menuOptions : List[Int] = List(1, 2);

    if (getUsers().forall(p => p._4)) {
      println("No BASIC users available to manage! Returning to the User Menu.");
      print("Press enter to continue...");
      readLine();
      state = "User Menu";
    }

    while (state.equals("Manage Users")) {
      val users : List[(Int, String, String, Boolean)] = getUsers();
      for(user : (Int, String, String, Boolean) <- users)
        if (!user._4)
          println(s"${user._1}. ${user._2} ${user._3}")
      println();
      AdminView.manageUsers();
      do {
        print("Please select a menu option, or enter 0 to return to the User Menu: ");
        try {
          userInput = readInt();
          if (!menuOptions.contains(userInput) && userInput != 0)
            println("Invalid menu selection! Please try again.");
        } catch {
          case nfe : NumberFormatException => {
            println("That is not a valid number! Please try again.");
          }
        }
      } while (!menuOptions.contains(userInput) && userInput != 0);
      if (userInput > 0) {
        var userChoice: Int = -1;
        do {
          userInput match {
            case 1 => print("Please select a user to delete, or enter 0 to return to User Management: ");
            case 2 => print("Please select a user to elevate to Admin status, or enter 0 to return to User Management: ");
          }
          try {
            userChoice = readInt();
            if (!users.exists(u => u._1 == userChoice) && userChoice != 0)
              println("Invalid user choice! Please try again.");
          } catch {
            case nfe: NumberFormatException => {
              println("That is not a valid number! Please try again.");
            }
          }
        } while (!users.exists(u => u._1 == userChoice) && userChoice != 0);

        if (userChoice > 0)
          userChoice match {
            case 1 => {
              deleteBasicUser(userChoice)
              if (getUsers().forall(p => p._4)) {
                println("No BASIC users available to manage! Returning to the User Menu.");
                print("Press enter to continue...");
                readLine();
                state = "User Menu";
              }
            }
            case 2 => elevateBasicUser(userChoice);
            case 0 => state = "User Menu";
          }

        userInput = -1;
      } else
        state = "User Menu";
    }
  }

  private def elevateBasicUser(user_id : Int) : Unit = {
    setToAdmin(user_id);
    if (isAdmin(user_id))
      println("User successfully promoted to Admin!");
    else
      println("User was elevated to Admin between selection and execution! Please select another user.");
    print("Press enter to continue...");
    readLine();
  }

  private def deleteBasicUser(user_id : Int) : Unit = {
    deleteUser(user_id);
    if (!getUsers().exists(u => u._1 == user_id))
      println("User successfully deleted!");
    else
      println("User was deleted between selection and execution! Please select another user.");
    print("Press enter to continue...");
    readLine();
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