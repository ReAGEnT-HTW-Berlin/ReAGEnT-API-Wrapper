name := "ReAGEnT-API-Wrapper"

version := "0.1"

scalaVersion := "2.12.14"
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

// stanford nlp for sentiment analysis
libraryDependencies += "edu.stanford.nlp" % "stanford-corenlp" % "4.2.2" classifier "models"

// disable trapExit, so systemd restarts the service upon operational disconnect or connection loss
fork in run := false
trapExit := false