-- MySQL dump 10.13  Distrib 5.5.33, for debian-linux-gnu (x86_64)
--
-- Host: 192.168.9.101    Database: meta
-- ------------------------------------------------------
-- Server version	5.5.31-1

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
  `id` int(11) DEFAULT NULL,
  `AccountID` int(11) DEFAULT NULL,
  `AgentName` varchar(50) DEFAULT NULL,
  `LastSyncTime` datetime DEFAULT NULL,
  `DataSize` int(11) DEFAULT NULL,
  `AgentUrl` varchar(80) DEFAULT NULL,
  `ConfigHash` varchar(50) DEFAULT NULL,
  `AgentState` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `TblApplication`
--

DROP TABLE IF EXISTS `TblApplication`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `TblApplication` (
  `ApplicationId` int(11) NOT NULL AUTO_INCREMENT,
  `ApplicationName` varchar(512) NOT NULL,
  `AccountId` int(11) DEFAULT NULL,
  PRIMARY KEY (`ApplicationId`)
) ENGINE=InnoDB AUTO_INCREMENT=7 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

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
  `QueryString` varchar(512) DEFAULT NULL,
  `EndTime` datetime DEFAULT NULL,
  `Error` varchar(512) DEFAULT NULL,
  `Url` varchar(255) DEFAULT NULL,
  `Duration` mediumtext,
  PRIMARY KEY (`QueryId`)
) ENGINE=InnoDB AUTO_INCREMENT=340 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

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
  PRIMARY KEY (`NameSpace`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `TblSavedQuery`
--

DROP TABLE IF EXISTS `TblSavedQuery`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `TblSavedQuery` (
  `QueryId` bigint(20) NOT NULL AUTO_INCREMENT,
  `QueryName` varchar(255) DEFAULT NULL,
  `QueryString` varchar(512) DEFAULT NULL,
  `CreatedUserId` int(11) DEFAULT NULL,
  `SubmitTime` datetime DEFAULT NULL,
  `AppName` varchar(30) DEFAULT NULL,
  `AppVersion` varchar(30) DEFAULT NULL,
  `DBName` varchar(30) DEFAULT NULL,
  PRIMARY KEY (`QueryId`)
) ENGINE=InnoDB AUTO_INCREMENT=37 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

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
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

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
) ENGINE=InnoDB AUTO_INCREMENT=7 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2013-10-30 15:59:17
