-- MySQL dump 10.13  Distrib 5.5.33, for debian-linux-gnu (x86_64)
--
-- Host: 192.168.9.101    Database: meta_stage
-- ------------------------------------------------------
-- Server version 5.5.31-1

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `TblAgent`
--

DROP TABLE IF EXISTS `TblAgent`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `TblAgent` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `AccountID` int(11) DEFAULT NULL,
  `AgentName` varchar(50) DEFAULT NULL,
  `LastSyncTime` datetime DEFAULT NULL,
  `DataSize` int(11) DEFAULT NULL,
  `AgentUrl` varchar(80) DEFAULT NULL,
  `AppName` varchar(30) DEFAULT NULL,
  `AppVersion` varchar(30) DEFAULT NULL,  
  `ConfigHash` varchar(50) DEFAULT NULL,
  `AgentState` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=20 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `TblAgent`
--

LOCK TABLES `TblAgent` WRITE;
/*!40000 ALTER TABLE `TblAgent` DISABLE KEYS */;
INSERT INTO `TblAgent` VALUES (19,5,'s3',NULL,NULL,'http://192.168.9.102:8081',NULL,NULL,NULL,1);
/*!40000 ALTER TABLE `TblAgent` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `TblApplication`
--

DROP TABLE IF EXISTS `TblApplication`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `TblApplication` (
  `ApplicationId` int(11) NOT NULL AUTO_INCREMENT,
  `ApplicationName` varchar(128) COLLATE utf8_unicode_ci DEFAULT NULL,
  `AccountId` int(11) DEFAULT NULL,
  PRIMARY KEY (`ApplicationId`),
  UNIQUE KEY `ApplicationName` (`ApplicationName`,`AccountId`)
) ENGINE=InnoDB AUTO_INCREMENT=36 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `TblApplication`
--

LOCK TABLES `TblApplication` WRITE;
/*!40000 ALTER TABLE `TblApplication` DISABLE KEYS */;
/*!40000 ALTER TABLE `TblApplication` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `TblHistoryQuery`
--

DROP TABLE IF EXISTS `TblHistoryQuery`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `TblHistoryQuery` (
  `QueryId` bigint(20) NOT NULL AUTO_INCREMENT,
  `SubmitUserId` int(11) NOT NULL,
  `SubmitTime` datetime DEFAULT NULL,
  `ExecutionStatus` int(11) DEFAULT NULL,
  `QueryString` TEXT DEFAULT NULL,
  `EndTime` datetime DEFAULT NULL,
  `Error` varchar(512) DEFAULT NULL,
  `Url` varchar(255) DEFAULT NULL,
  `Duration` longtext,
  PRIMARY KEY (`QueryId`)
) ENGINE=InnoDB AUTO_INCREMENT=893 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `TblHistoryQuery`
--

LOCK TABLES `TblHistoryQuery` WRITE;
/*!40000 ALTER TABLE `TblHistoryQuery` DISABLE KEYS */;
/*!40000 ALTER TABLE `TblHistoryQuery` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `TblMetaStore`
--

DROP TABLE IF EXISTS `TblMetaStore`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `TblMetaStore` (
  `NameSpace` varchar(100) NOT NULL,
  `AppName` varchar(30) DEFAULT NULL,
  `AppVersion` varchar(30) DEFAULT NULL,
  `DBName` varchar(20) DEFAULT NULL,
  `TableName` varchar(20) DEFAULT NULL,
  `hive_name` varchar(50) DEFAULT NULL,
  `NameSpaceType` int(11) NOT NULL,
  PRIMARY KEY (`NameSpace`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `TblMetaStore`
--

LOCK TABLES `TblMetaStore` WRITE;
/*!40000 ALTER TABLE `TblMetaStore` DISABLE KEYS */;
/*!40000 ALTER TABLE `TblMetaStore` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `TblSavedQuery`
--

DROP TABLE IF EXISTS `TblSavedQuery`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `TblSavedQuery` (
  `QueryId` bigint(20) NOT NULL AUTO_INCREMENT,
  `QueryName` varchar(255) DEFAULT NULL,
  `QueryString` text DEFAULT NULL,
  `CreatedUserId` int(11) DEFAULT NULL,
  `SubmitTime` datetime DEFAULT NULL,
  `AppName` varchar(30) DEFAULT NULL,
  `AppVersion` varchar(30) DEFAULT NULL,
  `DBName` varchar(30) DEFAULT NULL,
  PRIMARY KEY (`QueryId`)
) ENGINE=InnoDB AUTO_INCREMENT=56 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `TblSavedQuery`
--

LOCK TABLES `TblSavedQuery` WRITE;
/*!40000 ALTER TABLE `TblSavedQuery` DISABLE KEYS */;
/*!40000 ALTER TABLE `TblSavedQuery` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `TblSchema`
--

DROP TABLE IF EXISTS `TblSchema`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `TblSchema` (
  `NameSpace` varchar(100) DEFAULT NULL,
  `AgentID` varchar(50) DEFAULT NULL,
  `HasTimestamp` tinyint(1) DEFAULT NULL,
  `TimestampPosition` bigint(20) DEFAULT NULL,
  `TimestampKey` varchar(50) DEFAULT NULL,
  UNIQUE KEY `NameSpace` (`NameSpace`,`AgentID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `TblSchema`
--

LOCK TABLES `TblSchema` WRITE;
/*!40000 ALTER TABLE `TblSchema` DISABLE KEYS */;
/*!40000 ALTER TABLE `TblSchema` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `TblUsers`
--

DROP TABLE IF EXISTS `TblUsers`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `TblUsers` (
  `UserId` int(11) NOT NULL AUTO_INCREMENT,
  `Email` varchar(255) DEFAULT NULL,
  `Password` char(40) DEFAULT NULL,
  `AccountId` int(11) DEFAULT NULL,
  PRIMARY KEY (`UserId`)
) ENGINE=InnoDB AUTO_INCREMENT=6 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `TblUsers`
--

LOCK TABLES `TblUsers` WRITE;
/*!40000 ALTER TABLE `TblUsers` DISABLE KEYS */;
INSERT INTO `TblUsers` VALUES (4,'a@b.c','40bd001563085fc35165329ea1ff5c5ecbdbbeef',5),(5,'1@2.3','40bd001563085fc35165329ea1ff5c5ecbdbbeef',1);
/*!40000 ALTER TABLE `TblUsers` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `history_query`
--

DROP TABLE IF EXISTS `history_query`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `history_query` (
  `query_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `submit_user_id` int(11) NOT NULL,
  `query_string` varchar(512) DEFAULT NULL,
  `submit_time` datetime DEFAULT NULL,
  `execution_status` int(11) DEFAULT NULL,
  PRIMARY KEY (`query_id`)
) ENGINE=InnoDB AUTO_INCREMENT=7 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `history_query`
--

LOCK TABLES `history_query` WRITE;
/*!40000 ALTER TABLE `history_query` DISABLE KEYS */;
/*!40000 ALTER TABLE `history_query` ENABLE KEYS */;
UNLOCK TABLES;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2013-10-30 15:41:04

DROP TABLE IF EXISTS `TblTimingTaskLog`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `TblTimingTaskLog` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `TimingQueryID` int(11) NOT NULL ,
  `queryID` bigint(20) NOT NULL,
  `Status` varchar(50) NOT NULL,
  `Runtime` bigint(20) NOT NULL,

  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=20 DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS `TblTimingQuery`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `TblTimingQuery` (
  `TimingQueryID` int(11) NOT NULL AUTO_INCREMENT,
  `AccountID` int(11) DEFAULT NULL,
  `UserID` int(11) DEFAULT NULL,
  `TaskName` varchar(50) DEFAULT NULL,
  `AppName` varchar(30) DEFAULT NULL,
  `AppVersion` varchar(30) DEFAULT NULL,  
  `SqlString` text DEFAULT NULL,
  `StartTime` bigint(20) DEFAULT NULL,
  `EndTime` bigint(20) DEFAULT NULL,
  `TimeSpan` bigint(20) DEFAULT NULL,
  `FailMailFlag` varchar(10) DEFAULT NULL,
  `NoResultMailFlag` varchar(10) DEFAULT NULL,
  `AnyResultMailFlag` varchar(10) DEFAULT NULL,
  `MailList` text DEFAULT NULL,
  `ChartSetting` text DEFAULT NULL,
  PRIMARY KEY (`TimingQueryID`)
) ENGINE=InnoDB AUTO_INCREMENT=20 DEFAULT CHARSET=utf8;

/*!40101 SET character_set_client = @saved_cs_client */;


