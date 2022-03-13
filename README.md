# CONTENTS

 * Assessed Exercise
 * System and Environment Requirements
 * Running the project 
 * Maintainers
   * Supporting professors at organization

## Assessed Exercise
An Apache Spark application that takes input of a large set of text documents and a set of user defined queries, then for each query, rank the text documents by relevance for that query, as well as filter out any overly similar documents in the final ranking. The top 10 documents for each query are returned as output. 

### System and Environment Requirements
To access this project, your system needs the following software - 
1. Eclipse IDE
2. Java v11.x
3. Maven v3.8.x

### Running the application
To run the application locally, you need to run the `AssessedExercise.java` through the above-mentioned IDE.

To run the application remotely, you need to run the `RemoteSubmit.java` through the above-mentioned IDE.
 * To run on the full corpus, replace line no. 35 with - `boolean registerOk = RealizationEngineClient.registerApplication(args[0], args[1], "bdaefull");`
 * Else, to run on sample corpus, replave line no. 35 with - `boolean registerOk = RealizationEngineClient.registerApplication(args[0], args[1], "bdae");`

### Maintainers
* Tejas Kundu - https://stgit.dcs.gla.ac.uk/2647799k
* Garima Ashok Devnani - https://stgit.dcs.gla.ac.uk/2653434d
* Najmath Ummer - https://stgit.dcs.gla.ac.uk/2639140u (najmathu8@gmail.com)

#### Supporting professors at organization:
* Richard Mccreadie - https://stgit.dcs.gla.ac.uk/RichardMccreadie
