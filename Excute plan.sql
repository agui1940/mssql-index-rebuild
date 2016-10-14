/*#############################################
# Step2,整理索引第二步!在目标服务器上执行
# Author by WeiJiang
# Create Date:2016-9-20
# 
# 执行前检查事项:
# 1，PlanID存在,且内容正确；
# SELECT * FROM [subak01].[KFDB_Management].[dbo].[dbMaintenance] WHERE result=0 AND serverName=@@SERVERNAME ORDER BY startDTime DESC
# 
# 2，CheckPoint1 处,流水号正确,排序正确,过滤条件和结果正确,如有问题请修改CheckPoint1 处；
# SELECT a.planNo,a.serverName,[db_name],[object_name],index_name,b.index_type_desc,b.partition_number,page_count,CAST(b.page_count AS MONEY)*8/1024/1024 AS 'index_size_GB',
# 	fragment_count,CAST(b.fragment_count AS MONEY)*8/1024/1024 AS 'fragment_index_size_GB',avg_fragmentation_in_percent,type,script,b.startDTime,b.endDTime,b.elapsedTime
# 	FROM [SUBAK01].[KFDB_Management].[dbo].[dbMaintenance] a
# 	LEFT JOIN [SUBAK01].[KFDB_Management].[dbo].[dbMaintenanceDetail] b ON a.planID=b.planID
# 	WHERE [elapsedTime] = 0 AND a.result=0 AND a.serverName=@@SERVERNAME
# 	ORDER BY b.[type],b.fragment_count DESC,b.page_count DESC;
# 
# 3，CheckPoint2 ！！！此处默认注释，正式执行前请取消--EXEC (@script)注释；
#    --EXEC (@script); 
##############################################*/

USE master;
GO

SET NOCOUNT ON;
SET XACT_ABORT ON;
PRINT CONVERT(NVARCHAR(20), GETDATE(), 120) + ' Starting......';

/*定义变量*/
DECLARE @planID UNIQUEIDENTIFIER; --计划uniqueID
DECLARE @endDTime DATETIME; --结束时间
DECLARE @id INT; --脚本id
DECLARE @db_name NVARCHAR(100); --数据库名称
DECLARE @object_name NVARCHAR(100); --表名
DECLARE @index_name NVARCHAR(100); --索引名
DECLARE @page_count INT; --总页面数量
DECLARE @fragment_count INT; --碎片页面数量
DECLARE @avg_fragment_percent FLOAT; --碎片率
DECLARE @dTime DATETIME; --脚本执行时间
DECLARE @log_size FLOAT; --log日志文件大小
DECLARE @script NVARCHAR(MAX); --执行脚本
DECLARE @command NVARCHAR(MAX); --执行脚本

/*获取最新维护计划*/
SELECT TOP 1 @planID = planID,
       @endDTime = endDTime
  FROM [SUBAK01].[KFDB_Management].[dbo].[dbMaintenance]
 WHERE result     = 0
   AND serverName = @@SERVERNAME
 ORDER BY startDTime DESC;

/*定义游标，获取维护清单，优先rebuild，优先碎片从大到小，页面数从大到小*/






DECLARE partitions CURSOR
FOR
SELECT id,
       [db_name],
       [object_name],
       index_name,
       page_count,
       fragment_count,
       avg_fragmentation_in_percent,
       script
  FROM [SUBAK01].[KFDB_Management].[dbo].[dbMaintenanceDetail]
 WHERE planID        = @planID /*CheckPoint1 ！！！*/
   AND [elapsedTime] = 0
 ORDER BY [type],
          fragment_count DESC,
          page_count DESC;

OPEN partitions;
FETCH NEXT FROM partitions
INTO @id,
     @db_name,
     @object_name,
     @index_name,
     @page_count,
     @fragment_count,
     @avg_fragment_percent,
     @script;
WHILE @@FETCH_STATUS = 0
BEGIN
    SET @dTime = GETDATE();
    /*判断日志空间大小，过大时及时中止事务，防止磁盘爆满*/
	SET @command
        = 'SELECT @i=CONVERT(FLOAT, SUM(size)) * 8 / 1024.0 /1024.0 FROM ' + @db_name
          + '.dbo.sysfiles WHERE name LIKE ' + '''' + '%log' + '''';
    EXEC sys.sp_executesql @command, N'@i MONEY OUTPUT', @log_size OUTPUT;
    IF @log_size > 300
    BEGIN
        PRINT @db_name + '当前日志>300GB，' + CAST(@log_size AS NVARCHAR(10)) + 'GB;';
        PRINT '日志过大，磁盘空间有可能不足，请截断日志后重新运行！';
        /*关闭并跳出游标*/
        CLOSE partitions;
        DEALLOCATE partitions;
        RETURN;
    END;
    ELSE
    BEGIN
        PRINT @db_name + '当前日志<300GB，' + CAST(@log_size AS NVARCHAR(10)) + 'GB;';
    END;

    PRINT '开始整理索引：' + @db_name + ',' + @object_name + ',' + @index_name;
    PRINT '	' + CONVERT(NVARCHAR(20), GETDATE(), 120) + '	Srating...';
    PRINT '	page_count:' + CAST(@page_count AS NVARCHAR(10)) + ',fragment_count:'
          + CAST(@fragment_count AS NVARCHAR(10)) + ',avg_fragment_percent:'
          + CAST(@avg_fragment_percent AS NVARCHAR(10));
    PRINT '	' + @script;
    SELECT @script = 'USE ' + @db_name + ' ' + @script;
    /*开始事务处理*/
    BEGIN TRY
        BEGIN TRANSACTION;
        /*CheckPoint2 ！！！*/
        --EXEC (@script);
        /*更新维护记录,开始时间,结束时间,消耗用时*/
        UPDATE [SUBAK01].[KFDB_Management].[dbo].[dbMaintenanceDetail]
           SET startDTime = @dTime,
               endDTime = GETDATE(),
               elapsedTime = DATEDIFF(SECOND, @dTime, GETDATE())
         WHERE id = @id;
        /*更新维护项目,完成项目+1*/
        UPDATE [SUBAK01].[KFDB_Management].[dbo].[dbMaintenance]
           SET executedItem = executedItem + 1
         WHERE planID = @planID;
        PRINT '	' + CONVERT(NVARCHAR(20), GETDATE(), 120) + ' Done！事务完成！用时：'
              + CAST(DATEDIFF(SECOND, @dTime, GETDATE()) AS NVARCHAR(10)) + '秒';
        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH /*异常回滚*/
        ROLLBACK TRANSACTION;
        PRINT '	' + CONVERT(NVARCHAR(20), GETDATE(), 120) + ' 异常！事务回滚！用时：'
              + CAST(DATEDIFF(SECOND, @dTime, GETDATE()) AS NVARCHAR(10)) + '秒';
        /*关闭并跳出游标*/
        CLOSE partitions;
        DEALLOCATE partitions;
        RETURN;
    END CATCH;
    PRINT '	';
    FETCH NEXT FROM partitions
    INTO @id,
         @db_name,
         @object_name,
         @index_name,
         @page_count,
         @fragment_count,
         @avg_fragment_percent,
         @script;
END;
CLOSE partitions;
DEALLOCATE partitions;

/*更新维护计划*/
UPDATE [SUBAK01].[KFDB_Management].[dbo].[dbMaintenance]
   SET result = 1
 WHERE planID    = @planID
   AND totalItem = executedItem;

/*结束*/
PRINT CONVERT(NVARCHAR(20), GETDATE(), 120) + ' ENDING!!!';
