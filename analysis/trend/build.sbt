name:="trend"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
        "org.apache.spark" %% "spark-core" % "1.5.2" % "provided",
        "org.apache.spark" %% "spark-streaming" % "1.5.2" % "provided",
	"org.apache.spark" %% "spark-sql" % "1.5.2" % "provided"
)

javaOptions in run ++= Seq(
        "-Dlog4j.debug=true",
        "-Dlog4j.configuration=log4j.properties"
)
