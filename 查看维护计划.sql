--开始执行step2前，检查计划是否正确
--执行前检查事项:
--1，PlanID存在,且内容正确；
SELECT * FROM [subak01].[KFDB_Management].[dbo].[dbMaintenance] WHERE result=0 AND serverName=@@SERVERNAME ORDER BY startDTime DESC

--2，CheckPoint1 处,流水号正确,排序正确,过滤条件和结果正确,如有问题请修改CheckPoint1 处；
SELECT a.planNo,a.serverName,[db_name],[object_name],index_name,b.index_type_desc,b.partition_number,page_count,CAST(b.page_count AS MONEY)*8/1024/1024 AS 'index_size_GB',
	fragment_count,CAST(b.fragment_count AS MONEY)*8/1024/1024 AS 'fragment_index_size_GB',avg_fragmentation_in_percent,type,script,b.startDTime,b.endDTime,b.elapsedTime
	FROM [SUBAK01].[KFDB_Management].[dbo].[dbMaintenance] a
	LEFT JOIN [SUBAK01].[KFDB_Management].[dbo].[dbMaintenanceDetail] b ON a.planID=b.planID
	WHERE [elapsedTime] = 0 AND a.result=0 AND a.serverName=@@SERVERNAME
	ORDER BY b.[type],b.fragment_count DESC,b.page_count DESC;

--3，CheckPoint2 ！！！此处默认注释，正式执行前请取消--EXEC (@command)注释；
    --EXEC (@command);


--获取当前维护项目
SELECT * FROM [subak01].[KFDB_Management].[dbo].[dbMaintenance] WHERE result=0 AND serverName=@@SERVERNAME ORDER BY startDTime DESC

--已执行完的项目
SELECT b.id,a.planNo,a.serverName,[db_name],[object_name],index_name,b.index_type_desc,b.partition_number,page_count,CAST(b.page_count AS MONEY)*8/1024/1024 AS 'index_size_GB',
	fragment_count,CAST(b.fragment_count AS MONEY)*8/1024/1024 AS 'fragment_index_size_GB',avg_fragmentation_in_percent,type,script,b.startDTime,b.endDTime,b.elapsedTime
	FROM [SUBAK01].[KFDB_Management].[dbo].[dbMaintenance] a
	LEFT JOIN [SUBAK01].[KFDB_Management].[dbo].[dbMaintenanceDetail] b ON a.planID=b.planID
	WHERE [elapsedTime] > 0 AND a.serverName=@@SERVERNAME
	ORDER BY b.[type],b.fragment_count DESC,b.page_count DESC;

--还未执行的项目
SELECT a.planNo,a.serverName,[db_name],[object_name],index_name,b.index_type_desc,b.partition_number,page_count,CAST(b.page_count AS MONEY)*8/1024/1024 AS 'index_size_GB',
	fragment_count,CAST(b.fragment_count AS MONEY)*8/1024/1024 AS 'fragment_index_size_GB',avg_fragmentation_in_percent,type,script,b.startDTime,b.endDTime,b.elapsedTime
	FROM [SUBAK01].[KFDB_Management].[dbo].[dbMaintenance] a
	LEFT JOIN [SUBAK01].[KFDB_Management].[dbo].[dbMaintenanceDetail] b ON a.planID=b.planID
	WHERE [elapsedTime] = 0 AND a.serverName=@@SERVERNAME
	ORDER BY b.[type],b.fragment_count DESC,b.page_count DESC;