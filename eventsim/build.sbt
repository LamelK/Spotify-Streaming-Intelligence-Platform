name := "eventsim"

version := "1.0"

libraryDependencies += "org.apache.commons" % "commons-math3" % "3.5"

libraryDependencies += "de.jollyday" % "jollyday" % "0.5.10"

libraryDependencies += "org.rogach" %% "scallop" % "6.0.0"

libraryDependencies += "com.fasterxml.jackson.core" % "jackson-core" % "2.6.1"

libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.1"

libraryDependencies += "org.apache.kafka" % "kafka-clients" % "4.3.1"

// Silence "Failed to load class org.slf4j.impl.StaticLoggerBinder" that the
// Kafka client prints when no SLF4J binding is on the classpath.
libraryDependencies += "org.slf4j" % "slf4j-nop" % "1.7.36"

libraryDependencies += "com.lihaoyi" %% "upickle" % "3.1.0"

// Java 9+ dependencies (e.g. JAXB, pulled in by jollyday) each ship a
// module-info.class with different contents. We build one flat jar, so
// these are irrelevant and must be dropped to avoid merge conflicts.
assembly / assemblyMergeStrategy := {
  case PathList("module-info.class") => MergeStrategy.discard
  case x =>
    val oldStrategy = (assembly / assemblyMergeStrategy).value
    oldStrategy(x)
}
