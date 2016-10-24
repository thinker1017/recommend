name := "Homed RecommendSystem"

version := "2.1.alpha"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.4.1"
libraryDependencies += "commons-cli" % "commons-cli" % "1.2"
libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.33"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.4.1"
libraryDependencies += "net.debasishg" %% "redisclient" % "3.0"
libraryDependencies +=  "org.apache.spark"  % "spark-mllib_2.10" % "1.1.0"