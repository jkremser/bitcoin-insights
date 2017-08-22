/*
 * Copyright 2017 Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import sbt._
import Keys._


lazy val root = (project in file("."))
.settings(
    name := "bitcoin-insights",
    organization := "io.radanalytics",
    version := "0.0.1-SNAPSHOT",
    scalaVersion := "2.11.11"
)
 

resolvers += Resolver.mavenLocal

fork  := true

assemblyJarName in assembly := "bitcoin-insights.jar"

libraryDependencies += "com.github.zuinnote" % "hadoopcryptoledger-fileformat" % "1.0.4" % "compile"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.0" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-graphx" % "2.2.0" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.0" % "provided"

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.7.0" % "provided"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"
