/************************************************************
Step1,整理索引第一步!在目标服务器上执行,创建维护计划,生产维护脚本
Author by WeiJiang
Create Date:2016-9-20
************************************************************/
USE master;
GO

SET NOCOUNT ON;
SET XACT_ABORT ON;
PRINT CONVERT(NVARCHAR(20), GETDATE(), 120) + ' Starting......';

/*定义变量*/
DECLARE @startDTime DATETIME; --维护开始时间
DECLARE @hours INT; --维护时长
DECLARE @planID UNIQUEIDENTIFIER; --计划uniqueID
DECLARE @planNo NVARCHAR(50); --计划流水号
DECLARE @remark NVARCHAR(200); --计划备注
DECLARE @db_name NVARCHAR(50); --数据库名称
DECLARE @command NVARCHAR(MAX); --执行命令
DECLARE @dTime DATETIME;

/******************定义维护窗口,必填！！！*******************/
/************************************************************/
SET @startDTime = '2016-10-1 8:00'; /*必填,设置维护开始时间**/
SET @hours = 48; /*必填设置维护时长	   **/
/************************************************************/
/************************************************************/
SET @remark = CONVERT(NVARCHAR(20), GETDATE(), 120) + ',By ' + SYSTEM_USER; /*设置维护备注*/
SET @planID = NEWID(); /*生成唯一ID*/
SET @planNo = CONVERT(NVARCHAR(10), @startDTime, 112); /*生成流水号，格式yyyymmdd*/

/*判断是否存在相同流水号*/
IF EXISTS (SELECT *
             FROM [SUBAK01].[KFDB_Management].[dbo].[dbMaintenance]
            WHERE planNo     = @planNo
              AND serverName = SERVERPROPERTY('ServerName'))
BEGIN
    PRINT '	错误，已存在相同流水号!';
    RETURN;
END;

/*判断是否有未完成的维护任务*/
IF EXISTS (SELECT *
             FROM [SUBAK01].[KFDB_Management].[dbo].[dbMaintenance]
            WHERE result     = 0
              AND serverName = SERVERPROPERTY('ServerName'))
BEGIN
    PRINT '	错误，有未完成的维护任务!';
    RETURN;
END;

/*生成维护项目*/
INSERT [SUBAK01].[KFDB_Management].[dbo].[dbMaintenance]
SELECT @planID,
       @planNo,
       CAST(SERVERPROPERTY('ServerName') AS NVARCHAR(50)),
       'All user database',
       0,
       0,
       @startDTime,
       DATEADD(HH, @hours, @startDTime),
       '',
       @remark;

/*定义游标，循环数据库，生成维护脚本*/
DECLARE partitions CURSOR
FOR
SELECT name
  FROM master..sysdatabases
 WHERE dbid > 5
   AND name NOT LIKE '%MES%'; /*这里可以排除不需要的数据库！！！*/

OPEN partitions;
FETCH NEXT FROM partitions
INTO @db_name;
WHILE @@FETCH_STATUS = 0
BEGIN
    SET @dTime = GETDATE();
    PRINT '	' + CONVERT(NVARCHAR(20), GETDATE(), 120) + '开始评估索引情况. ' + @db_name;
    SET @command
        = 'USE ' + @db_name + '	SELECT '    + '''' + CAST(@planID AS NVARCHAR(50)) + ''''
          + ',DB_NAME(a.database_id) AS db_name,c.name+N''.''+OBJECT_NAME(a.object_id) AS object_name,d.name AS index_name,b.type_desc,d.type_desc AS index_type_desc,d.is_unique,d.data_space_id,d.ignore_dup_key,d.is_primary_key,a.index_id,a.partition_number,a.index_depth,a.index_level,a.avg_fragmentation_in_percent,a.fragment_count,a.avg_fragment_size_in_pages,a.page_count,
		CASE WHEN a.avg_fragmentation_in_percent< 30.0 THEN '+''''+'Reorganize'+''''+' ELSE '+''''+'Rebuild'+''''
          + ' END AS [type],
		CASE WHEN a.avg_fragmentation_in_percent< 30.0 THEN '+''''+'ALTER INDEX '+''''+' + d.Name + '+''''+' ON '+''''+' + c.NAME + N''.'' + b.NAME + '
          + '''' + ' REORGANIZE ' + '''' + '
			  ELSE '+''''+'ALTER INDEX ['+''''+' + d.Name + '+''''+ '] ON [' + '''' + ' + c.NAME + ' + '''' + '].['
          + '''' + ' + b.NAME + ' + '''' + '] REBUILD' + '''' + ' END +
		CASE WHEN partitioncount> 1 THEN '+''''+' PARTITION='+''''+' + rtrim ( a.partition_number)+ '+''''+';'+''''+' ELSE '+''''
          + ';' + ''''
          + ' END AS script,NULL,NULL,0
	FROM sys.dm_db_index_physical_stats ( DB_ID(), NULL, NULL , NULL, '+''''+'LIMITED'+''''
          + ' ) AS a
		INNER JOIN sys.objects AS b ON a.object_id = b.object_id
		INNER JOIN sys.schemas AS c ON c.schema_id = b.schema_id
		INNER JOIN sys.indexes AS d ON d.object_id = a.object_id AND d.index_id= a.index_id
		INNER JOIN (SELECT object_id , index_id, partitioncount= COUNT (*) FROM sys.partitions 
						GROUP BY object_id , index_id) AS e ON e.object_id = a.object_id AND e.index_id= a.index_id
	WHERE a.avg_fragmentation_in_percent > 10.0 AND a.index_id > 0 AND d. Name IS NOT NULL AND a.fragment_count>1024';

    INSERT INTO [SUBAK01].[KFDB_Management].[dbo].[dbMaintenanceDetail] ([planID],
                                                                         [db_name],
                                                                         [object_name],
                                                                         [index_name],
                                                                         [type_desc],
                                                                         [index_type_desc],
                                                                         [is_unique],
                                                                         [data_space_id],
                                                                         [ignore_dup_key],
                                                                         [is_primary_key],
                                                                         [index_id],
                                                                         [partition_number],
                                                                         [index_depth],
                                                                         [index_level],
                                                                         [avg_fragmentation_in_percent],
                                                                         [fragment_count],
                                                                         [avg_fragment_size_in_pages],
                                                                         [page_count],
                                                                         [type],
                                                                         [script],
                                                                         [startDTime],
                                                                         [endDTime],
                                                                         [elapsedTime])
    EXEC(@command);
    PRINT '		' + '用时:' + CAST(DATEDIFF(SECOND, @dTime, GETDATE()) AS NVARCHAR(10)) + '秒';
    PRINT '	';
    FETCH NEXT FROM partitions
    INTO @db_name;
END;
CLOSE partitions;
DEALLOCATE partitions;

/*更新维护项目中，需要整理的索引数量*/
UPDATE [SUBAK01].[KFDB_Management].[dbo].[dbMaintenance]
   SET totalItem = (SELECT COUNT(1)
                      FROM [SUBAK01].[KFDB_Management].[dbo].[dbMaintenanceDetail]
                     WHERE planID = @planID)
 WHERE planID = @planID;

/*结束*/
PRINT CONVERT(NVARCHAR(20), GETDATE(), 120) + ' ENDING!!!';