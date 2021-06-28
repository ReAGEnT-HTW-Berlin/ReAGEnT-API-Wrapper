name := "ReAGEnT-API-Wrapper"

version := "0.1"

scalaVersion := "2.12.14"
//val SparkVersion = "2.4.7"
val SparkVersion = "2.4.8"
libraryDependencies ++= Seq(
	// Spark Dependencies
	"org.apache.spark" %% "spark-core" % SparkVersion,
	"org.apache.spark" %% "spark-streaming" % SparkVersion,
	"org.apache.spark" %% "spark-sql" % SparkVersion,
	"org.apache.spark" %% "spark-catalyst" % SparkVersion,
	// MongoDB-Spark-Connector
	"org.mongodb.spark" %% "mongo-spark-connector" % "2.4.3",
	//	"org.reactivemongo" %% "reactivemongo" % "1.0.3",
	"org.mongodb.scala" %% "mongo-scala-driver" % "2.9.0",
	// JSON
	//	"io.spray" %%  "spray-json" % "1.3.6",
	// Configuration
	"com.typesafe" % "config" % "1.4.1",
	// Http Library
	"org.scalaj" %% "scalaj-http" % "2.4.2",
	// Tests
	"org.scalactic" %% "scalactic" % "3.2.5",
	"org.scalatest" %% "scalatest" % "3.2.5" % "test",
	// Sentiment Analysis
	"edu.stanford.nlp" % "stanford-corenlp" % "4.2.2"
)
libraryDependencies += "edu.stanford.nlp" % "stanford-corenlp" % "4.2.2" classifier "models"

fork in run := false
trapExit := false