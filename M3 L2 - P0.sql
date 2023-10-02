
CREATE VIEW dbo.vSkew
AS
WITH TblDtls 
AS
(
SELECT	t.[name]				AS  [table_name],
		nt.[distribution_id]	AS  [distribution_id],
		nps.[row_count]			AS  [row_count]
	FROM sys.tables t
		INNER JOIN sys.pdw_table_mappings tm
			ON t.[object_id] = tm.[object_id]
		INNER JOIN sys.pdw_nodes_tables nt
			ON tm.[physical_name] = nt.[name]
		INNER JOIN sys.dm_pdw_nodes pn
			ON  nt.[pdw_node_id] = pn.[pdw_node_id]
		INNER JOIN sys.dm_pdw_nodes_db_partition_stats nps
			ON nt.[object_id] = nps.[object_id]
			AND nt.[pdw_node_id] = nps.[pdw_node_id]
			AND nt.[distribution_id] = nps.[distribution_id]
	WHERE pn.[type] = 'COMPUTE'
)
SELECT	table_name,
		( ((MAX(row_count) * 1.0) - (MIN(row_count) * 1.0)) / (SUM(row_count) * 1.0) ) * 100.0 AS Skew
	FROM tblDtls
	GROUP BY table_name



GO

CREATE VIEW vCCIHEalth 
AS

SELECT               
	s.name															as 'Schema_Name',
    t.name															as 'Table_Name',
	tdp.distribution_policy_desc									as 'Distribution_type',
	SUM(rg.Total_rows)												as 'Total_Rows',
	pt.max_column_id_used											as 'Column_Count',
	--MAX(p.partition_number)											as 'Partition_Count',
    SUM(CASE WHEN rg.State = 1 THEN 1 else 0 end)					as 'OPEN_Row_Groups',
    SUM(CASE WHEN rg.State = 1 THEN rg.Total_rows else 0 end)		as 'OPEN_rows',
    MIN(CASE WHEN rg.State = 1 THEN rg.Total_rows else NULL end)	as 'MIN OPEN Row Group Rows',
    MAX(CASE WHEN rg.State = 1 THEN rg.Total_rows else NULL end)	as 'MAX OPEN_Row Group Rows',
    AVG(CASE WHEN rg.State = 1 THEN rg.Total_rows else NULL end)	as 'AVG OPEN_Row Group Rows',
 
    SUM(CASE WHEN rg.State = 3 THEN 1 else 0 end)					as 'COMPRESSED_Row_Groups',
    SUM(CASE WHEN rg.State = 3 THEN rg.Total_rows else 0 end)		as 'COMPRESSED_Rows',
	SUM(CASE WHEN rg.State = 3 THEN rg.deleted_rows else 0 end)		as 'Deleted_COMPRESSED_Rows',
	MIN(CASE WHEN rg.State = 3 THEN rg.Total_rows else NULL end)	as 'MIN COMPRESSED Row Group Rows',
    MAX(CASE WHEN rg.State = 3 THEN rg.Total_rows else NULL end)	as 'MAX COMPRESSED Row Group Rows',
    AVG(CASE WHEN rg.State = 3 THEN rg.Total_rows else NULL end)	as 'AVG_COMPRESSED_Rows',
 
    SUM(CASE WHEN rg.State = 2 THEN 1 else 0 end)					as 'CLOSED_Row_Groups',
    SUM(CASE WHEN rg.State = 2 THEN rg.Total_rows else 0 end)		as 'CLOSED_Rows',
	MIN(CASE WHEN rg.State = 2 THEN rg.Total_rows else NULL end)	as 'MIN CLOSED Row Group Rows',
    MAX(CASE WHEN rg.State = 2 THEN rg.Total_rows else NULL end)	as 'MAX CLOSED Row Group Rows',
    AVG(CASE WHEN rg.State = 2 THEN rg.Total_rows else NULL end)	as 'AVG CLOSED Row Group Rows'
FROM sys.dm_pdw_nodes_db_column_store_row_group_physical_stats rg
INNER JOIN sys.pdw_nodes_tables pt
	ON rg.object_id = pt.object_id
	AND rg.pdw_node_id = pt.pdw_node_id
	AND rg.distribution_id = pt.distribution_id
INNER JOIN sys.pdw_table_mappings mp
	ON pt.name = mp.physical_name
INNER JOIN sys.tables t
	ON     mp.object_id = t.object_id
INNER JOIN sys.schemas s
    ON t.schema_id = s.schema_id
INNER JOIN sys.pdw_table_distribution_properties tdp
	ON tdp.object_id = t.object_id
GROUP BY t.name,s.name,tdp.distribution_policy_desc,pt.max_column_id_used

