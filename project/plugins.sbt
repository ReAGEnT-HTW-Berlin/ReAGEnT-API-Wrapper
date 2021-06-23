//addSbtPlugin("com.typesafe.sbteclipse" % "sbteclipse-plugin" % "5.2.4")
////addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.15.0")
////enablePlugins(SystemVPlugin)
////Resolver.typesafeIvyRepo("releases")
//addSbtPlugin("com.typesafe.sbt" % "sbt-native-packager" % "1.8.1")

resolvers += Resolver.url("myyk-bintray-sbt-plugins", url("https://dl.bintray.com/myyk/sbt-plugins/"))(Resolver.ivyStylePatterns)
addSbtPlugin("com.typesafe.sbt" % "sbt-native-packager" % "1.8.1")

//libraryDependencies += "org.vafer" % "jdeb" % "1.3" artifacts (Artifact("jdeb", "jar", "jar"))
//addSbtPlugin("com.typesafe.sbt" % "sbt-jshint" % "1.0.6")