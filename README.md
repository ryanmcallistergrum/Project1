# Project1
## Project Description
Project1 fetches video game news data from GameSpot's API in real-time, storing it in Hive, and analyzes it using a command-line interface application written in Scala with a user management and access system for basic and admin users.

## Technologies Used
- Java - version 1.8.0_311
- Scala - version 2.12.15
- Spark - version 3.1.2
- Spark SQL - version 3.1.2
- Hive - version 3.1.2
- HDFS - version 3.3.0
- Git + GitHub

## Features
- Login and new basic user creation
- Restricted access for basic users, full access for admins
- Continuous fetching and reporting of new data from GameSpot's REST API
- Create, rename, delete analytical SQL queries
  - Cannot add, update, or delete data from queries
  - Can export query results to JSON (writes file for each partition and merges afterwards)
- Promote basic users to admins, and delete basic users
- Can change own username and password
- Passwords stored and verified as hashes
- Default admin account created on initialization
- Hive tables implement bucketing and partitioning

## Getting Started
- (Note, these instructions only support Windows 10 and above)
- First, download and install Git
    - For Windows, navigate to https://git-scm.com/download/win and install
- Run the following Git command to create a copy of the project repository using either Command Prompt or PowerShell:
    - git clone https://github.com/ryanmcallistergrum/Project1.git
- Install Java
    - Navigate to https://www.oracle.com/java/technologies/javase/javase8u211-later-archive-downloads.html and install
- Install IntelliJ
  - Navigate to https://www.jetbrains.com/idea/download/download-thanks.html?platform=windows&code=IIC and install
- Create GameSpot Developer Account to get API Key
  - In https://www.gamespot.com/api/, create an account to get an API key
  - Insert the API key into the apiDetails.txt file located at src/main/scala
    - Also add a unique UserAgent

## Usage
- Right-click on the repository, select "Open Folder as IntelliJ IDEA Community Edition Project", and then run it by selecting "Run -> Run" at the top.
- General Usage
  - Either log in using the default admin account (password is admin), or create a new basic user account
    - ![Login Menu](/images/Login%20Menu.png?!raw=true)
    - ![Creating a New Basic User](/images/Create%20New%20User.png?raw=true)
    - ![What Happens When User Already Exists](/images/User%20Already%20Exists.png?raw=true)
  - After successful login, whether admin or basic, the user will reach the Home Screen, where the user can watch new games, articles, and reviews come in from GameSpot's API
    - ![Home Screen After Successful Login](/images/Successful%20Login,%20Home%20Screen.png?raw=true)
  - 