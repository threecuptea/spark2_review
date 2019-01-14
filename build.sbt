name := "spark2_review"

version := "0.1"

scalaVersion := "2.11.11"

javacOptions ++= Seq("-source", "1.8")
compileOrder := CompileOrder.JavaThenScala

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.0" % "provided",
  "org.apache.spark" %% "spark-sql"  % "2.4.0" % "provided",
  "org.apache.spark" %% "spark-mllib"  % "2.4.0" % "provided",
  "org.mongodb.spark" %% "mongo-spark-connector"  % "2.4.0" % "provided"
)