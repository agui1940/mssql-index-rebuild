USE KFDB_Mes_CSBU3;
SELECT DB_NAME(a.database_id) AS db_name,
       c.name + N'.' + OBJECT_NAME(a.object_id) AS object_name,
       d.name AS index_name,
       b.type_desc,
       d.type_desc AS index_type_desc,
       d.is_unique,
       d.data_space_id,
       d.ignore_dup_key,
       d.is_primary_key,
       a.index_id,
       a.partition_number,
       a.index_depth,
       a.index_level,
       a.avg_fragmentation_in_percent,
       a.fragment_count,
       a.avg_fragment_size_in_pages,
       a.page_count,
       CASE
            WHEN a.avg_fragmentation_in_percent < 30.0 THEN 'Reorganize'
            ELSE 'Rebuild' END AS [type],
       CASE
            WHEN a.avg_fragmentation_in_percent < 30.0 THEN 'ALTER INDEX ' + d.name + ' ON ' + c.name + N'.' + b.name
                                                            + ' REORGANIZE '
            ELSE 'ALTER INDEX [' + d.name + '] ON [' + c.name + '].[' + b.name + '] REBUILD' END
       + CASE
              WHEN partitioncount > 1 THEN ' PARTITION=' + RTRIM(a.partition_number) + ';'
              ELSE ';' END AS script,
       NULL,
       NULL,
       0
  FROM sys.dm_db_index_physical_stats(
               DB_ID(),
               NULL,
               NULL,
               NULL,
               'LIMITED') AS a
 INNER JOIN sys.objects AS b
    ON a.object_id = b.object_id
 INNER JOIN sys.schemas AS c
    ON c.schema_id = b.schema_id
 INNER JOIN sys.indexes AS d
    ON d.object_id = a.object_id
   AND d.index_id  = a.index_id
 INNER JOIN (SELECT object_id,
                    index_id,
                    partitioncount = COUNT(*)
               FROM sys.partitions
              GROUP BY object_id,
                       index_id) AS e
    ON e.object_id = a.object_id
   AND e.index_id  = a.index_id
 WHERE a.avg_fragmentation_in_percent > 10.0
   AND a.index_id                     > 0
   AND d.name IS NOT NULL;