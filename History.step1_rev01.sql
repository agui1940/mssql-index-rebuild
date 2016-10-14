/*
Author by WeiJiang
Create Date:2016-9-20
创建维护计划		（在需要整理索引的数据库服务器上执行）
-----------------------------------------------------------------------------------------------------------------------
*/
SET NOCOUNT ON;
PRINT CONVERT(NVARCHAR(20),GETDATE(),120) +' Starting......' 
USE master
GO
--创建临时表
IF OBJECT_ID('Tempdb..#indexInfo') IS NOT NULL
	DROP TABLE #indexInfo
GO
CREATE TABLE #indexInfo(
	[db_name] [nvarchar](100) NOT NULL,
	[object_name] [nvarchar](100) NOT NULL,
	[index_name] [nvarchar](100) NOT NULL,
	[type_desc] [nvarchar](100) NULL,
	[index_type_desc] [nvarchar](100) NULL,
	[is_unique] [bit] NULL,
	[data_space_id] [int] NULL,
	[ignore_dup_key] [bit] NULL,
	[is_primary_key] [bit] NULL,
	[index_id] [int] NOT NULL,
	[partition_number] [int] NOT NULL,
	[index_depth] [tinyint] NULL,
	[index_level] [tinyint] NULL,
	[avg_fragmentation_in_percent] [float] NULL,
	[fragment_count] [bigint] NULL,
	[avg_fragment_size_in_pages] [float] NULL,
	[page_count] [bigint] NULL,
	[type] [nvarchar](50) NULL,
	[script] [nvarchar](max) NOT NULL)

--创建临时存储过程
IF OBJECT_ID ('SP_GetIndexInfo','P') IS NOT NULL
	DROP PROCEDURE SP_GetIndexInfo
GO
CREATE PROCEDURE SP_GetIndexInfo
AS
	IF DB_ID()<5
		RETURN
	DECLARE @dTime DATETIME
	SET @dTime=GETDATE();
	PRINT '	'+CONVERT(NVARCHAR(20),GETDATE(),120)+' '+DB_name()+'开始评估索引情况.';
	SELECT DB_NAME(a.database_id) AS db_name,c.name+'.'+OBJECT_NAME(a.object_id) AS object_name,d.name AS index_name,b.type_desc,d.type_desc AS index_type_desc,d.is_unique,d.data_space_id,d.ignore_dup_key,d.is_primary_key,a.index_id,a.partition_number,a.index_depth,a.index_level,a.avg_fragmentation_in_percent,a.fragment_count,a.avg_fragment_size_in_pages,a.page_count,--b.create_date,b.modify_date,
		CASE WHEN a.avg_fragmentation_in_percent< 30.0 THEN 'Reorganize' ELSE 'Rebuild' END AS [type],
		CASE WHEN a.avg_fragmentation_in_percent< 30.0 THEN 'ALTER INDEX ' + d.Name + ' ON ' + c.NAME + '.' + b.NAME + ' REORGANIZE '
			  ELSE 'ALTER INDEX [' + d.Name + '] ON [' + c.NAME + '].[' + b.NAME + '] REBUILD' END +
		CASE WHEN partitioncount> 1 THEN ' PARTITION=' + rtrim ( a.partition_number)+ ';' ELSE ';' END AS script
	FROM sys.dm_db_index_physical_stats ( DB_ID(), NULL, NULL , NULL, 'LIMITED' ) AS a
		INNER JOIN sys.objects AS b ON a.object_id = b.object_id
		INNER JOIN sys.schemas AS c ON c.schema_id = b.schema_id
		INNER JOIN sys.indexes AS d ON d.object_id = a.object_id AND d.index_id= a.index_id
		INNER JOIN (SELECT object_id , index_id, partitioncount= COUNT (*) FROM sys.partitions 
						GROUP BY object_id , index_id) AS e ON e.object_id = a.object_id AND e.index_id= a.index_id
	WHERE a.avg_fragmentation_in_percent > 10.0 AND a.index_id > 0 AND d. Name IS NOT NULL
	PRINT '		'+'Done！用时：'+CAST(DATEDIFF(SECOND,@dTime,GETDATE()) AS NVARCHAR(10))+'秒';
	PRINT '	';
GO

--定义变量
DECLARE @startDTime DATETIME;			--维护开始时间
DECLARE @hours int;						--维护时长
DECLARE @planID uniqueidentifier;		--计划uniqueID
DECLARE @planNo nvarchar(50);			--计划流水号
DECLARE @remark nvarchar(200);			--计划备注

--定义维护窗口
SET @startDTime='2016-10-1 8:00';		--设置维护开始时间
SET @hours=48;							--设置维护时长
SET @remark=CONVERT(NVARCHAR(20),GETDATE(),120)+',By '+system_user;			--设置维护备注
SET @planID=NEWID();					--生产唯一ID
SET @planNo=CONVERT(nvarchar(10),@startDTime,112);	--生成流水号

IF EXISTS (SELECT * FROM [SUBAK01].[KFDB_Management].[dbo].[dbMaintenance] WHERE planNo=@planNo AND ServerName=SERVERPROPERTY('ServerName'))
BEGIN
	PRINT '	错误，已存在相同流水号!';
	RETURN;
END

IF EXISTS (SELECT * FROM [SUBAK01].[KFDB_Management].[dbo].[dbMaintenance] WHERE result=0 AND ServerName=SERVERPROPERTY('ServerName'))
BEGIN
	PRINT '	错误，有未完成的维护任务!';
	RETURN;
END

--插入维护项目
INSERT [SUBAK01].[KFDB_Management].[dbo].[dbMaintenance]
SELECT @planID,@planNo,CAST(SERVERPROPERTY('ServerName') AS nvarchar(50)),'All user database',0,0,@startDTime,DATEADD(HH,@hours,@startDTime),'',@remark
	
--标记为系统存储过程
EXEC sp_MS_marksystemobject 'SP_GetIndexInfo'
	
--遍历实例所有数据库，生成维护计划
INSERT INTO #indexInfo
EXEC sp_MSforeachdb 'use [?] exec SP_GetIndexInfo'
	
--插入维护计划
INSERT [SUBAK01].[KFDB_Management].[dbo].[dbMaintenanceDetail] ([planID],[db_name],[object_name],[index_name],[type_desc],[index_type_desc],[is_unique],[data_space_id],[ignore_dup_key],[is_primary_key],[index_id],[partition_number],[index_depth],[index_level],[avg_fragmentation_in_percent],[fragment_count],[avg_fragment_size_in_pages],[page_count],[type],[script],[startDTime],[endDTime],[elapsedTime])
SELECT @planID,*,NULL,NULL,NULL FROM #indexInfo
	
--更新需要整理的索引数量
UPDATE [SUBAK01].[KFDB_Management].[dbo].[dbMaintenance] SET totalItem=(SELECT COUNT(0) FROM #indexInfo)

--结束
PRINT CONVERT(NVARCHAR(20),GETDATE(),120) +' ENDING!!!';

