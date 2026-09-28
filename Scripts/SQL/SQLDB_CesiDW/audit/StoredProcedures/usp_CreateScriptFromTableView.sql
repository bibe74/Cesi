
/**
 * @storedprocedure audit.usp_CreateScriptFromTableView
*/

CREATE   PROCEDURE audit.usp_CreateScriptFromTableView (
    @schemaName sysname,
    @tableName sysname,
    @updatedatetimeColumn sysname = 'UpdateDatetime',
    @isDeletedColumn sysname = 'IsDeleted'
)
AS
BEGIN

    DECLARE @version VARCHAR(10) = '20260722.0';

    SET NOCOUNT ON;

    DECLARE @viewName sysname = CONCAT(@tableName, 'View');
    DECLARE @viewFullName sysname = CONCAT(@schemaName, '.', @viewName);

    IF OBJECT_ID(@viewFullName, 'V') IS NULL
    BEGIN
        RAISERROR('ERROR: view %s does not exist. Exiting', 16, 1, @viewFullName);
        RETURN;
    END;

    DROP TABLE IF EXISTS #DWHColumns;

    SELECT
        'HistoricalHashKey' AS ColumnName

    INTO #DWHColumns

    UNION ALL SELECT 'HistoricalHashKeyASCII'
    UNION ALL SELECT 'ChangeHashKey'
    UNION ALL SELECT 'ChangeHashKeyASCII'
    UNION ALL SELECT 'InsertDatetime'
    UNION ALL SELECT @updatedatetimeColumn
    UNION ALL SELECT @isDeletedColumn;

    DROP TABLE IF EXISTS #viewColumns;

    SELECT
        c.column_id,
        c.name AS ColumnName,
        ty.name AS DataType,
        c.max_length,
        c.precision,
        c.scale,
        c.is_nullable
    
    INTO #viewColumns
    FROM sys.views v
    INNER JOIN sys.schemas S ON S.schema_id = v.schema_id
        AND S.name = @schemaName
    INNER JOIN sys.columns c ON v.object_id = c.object_id
    INNER JOIN sys.types ty ON c.user_type_id = ty.user_type_id
    WHERE v.name = @viewName
    ORDER BY c.column_id;

    DECLARE @minDWHColumnID INT,
        @maxDWHColumnID INT,
        @changeHashKeyColumn NVARCHAR(128) = 'ChangeHashKey',
        @changeHashKeyColumnID INT;

    SELECT
        @minDWHColumnID = MIN(C.column_id),
        @maxDWHColumnID = MAX(C.column_id),
        @changeHashKeyColumnID = MAX(CASE WHEN C.ColumnName = @changeHashKeyColumn THEN C.column_id ELSE NULL END)

    FROM #viewColumns C
    INNER JOIN #DWHColumns DWHC ON DWHC.ColumnName = C.ColumnName;

    IF (@minDWHColumnID IS NULL)
    BEGIN
        RAISERROR('ERROR: no DWH columns in view %s. Exiting', 16, 1, @viewFullName);
        RETURN;
    END;

    IF (@changeHashKeyColumnID IS NULL)
    BEGIN
        RAISERROR('ERROR: no %s column in view %s. Exiting', 16, 1, @changeHashKeyColumn, @viewFullName);
        RETURN;
    END;

    DECLARE @targetTableSchema NVARCHAR(128) = @schemaName;
    DECLARE @targetTableName NVARCHAR(128) = @tableName;
    DECLARE @mergeSPName NVARCHAR(256) = 'usp_Merge_' + @tableName;

    DECLARE @SchemaAndName NVARCHAR(256) = @viewFullName;
    DECLARE @targetSchemaAndName NVARCHAR(256) = @targetTableSchema + '.' + @targetTableName;
    DECLARE @mergeSPSchemaAndName NVARCHAR(256) = @targetTableSchema + '.' + @mergeSPName;

    DECLARE @SQLScript NVARCHAR(MAX) = '';
    --DECLARE @ColumnDefsForTable NVARCHAR(MAX) = '';
    DECLARE @PKColumnsList NVARCHAR(MAX) = '';
    DECLARE @DWHColumnsListForUpdate NVARCHAR(MAX) = '';
    DECLARE @nonPKColumnsListForUpdate NVARCHAR(MAX) = '';
    DECLARE @PKColumnsListForLog NVARCHAR(MAX) = '';
    DECLARE @allColumnsList NVARCHAR(MAX) = '';
    DECLARE @joinCondition NVARCHAR(MAX) = '';
    DECLARE @firstPKColumn NVARCHAR(128);

    -- Genera le definizioni delle colonne per la tabella e le liste di colonne
    SELECT
        @allColumnsList = @allColumnsList + VC.ColumnName + ',',
        @firstPKColumn = CASE WHEN VC.column_id = 1 THEN VC.ColumnName ELSE @firstPKColumn END,
        @PKColumnsList = @PKColumnsList +
            CASE
                WHEN column_id < @minDWHColumnID THEN VC.ColumnName + ','
                ELSE ''
            END,
        @nonPKColumnsListForUpdate = @nonPKColumnsListForUpdate +
            CASE
                WHEN VC.column_id > @maxDWHColumnID THEN 'TGT.' + VC.ColumnName + ' = SRC.' + VC.ColumnName + ','
                ELSE ''
            END,
        @DWHColumnsListForUpdate = @DWHColumnsListForUpdate +
            CASE
                WHEN VC.column_id BETWEEN @minDWHColumnID AND @maxDWHColumnID AND (VC.ColumnName IN (@updatedatetimeColumn, @isDeletedColumn) OR LEFT(VC.ColumnName, LEN(@changeHashKeyColumn)) = @changeHashKeyColumn) THEN 'TGT.' + VC.ColumnName + ' = SRC.' + VC.ColumnName + ','
                ELSE ''
            END,
        @joinCondition = @joinCondition +
            CASE
                WHEN VC.column_id < @minDWHColumnID THEN ' SRC.' + VC.ColumnName + ' = TGT.' + VC.ColumnName + ' AND'
                ELSE ''
            END,
        @PKColumnsListForLog = @PKColumnsListForLog + CASE WHEN VC.column_id < @minDWHColumnID THEN ' + ''' + VC.ColumnName + ' = '' + CAST(COALESCE(inserted.' + VC.ColumnName + ', deleted.' + VC.ColumnName + ') AS NVARCHAR)' ELSE '' END

    FROM #viewColumns VC
    ORDER BY VC.column_id;

    SET @allColumnsList = LEFT(@allColumnsList, LEN(@allColumnsList) - 1); -- Rimuove la virgola finale
    SET @PKColumnsList = LEFT(@PKColumnsList, LEN(@PKColumnsList) - 1); -- Rimuove la virgola finale
    SET @nonPKColumnsListForUpdate = LEFT(@nonPKColumnsListForUpdate, LEN(@nonPKColumnsListForUpdate) - CASE WHEN LEN(@nonPKColumnsListForUpdate) = 0 THEN 0 ELSE 1 END); -- Rimuove la virgola finale
    SET @PKColumnsListForLog = RIGHT(@PKColumnsListForLog, LEN(@PKColumnsListForLog) - 2); -- Rimuove ', ' iniziale
    SET @joinCondition = LEFT(@joinCondition, LEN(@joinCondition) - 3); -- Rimuove ' AND' finale

    SET @SQLScript = '--DROP TABLE IF EXISTS ' + @targetSchemaAndName + ';
GO

IF OBJECT_ID(''' + @targetSchemaAndName + ''', ''U'') IS NULL
BEGIN
    SELECT TOP (0) * INTO ' + @targetSchemaAndName + ' FROM ' + @SchemaAndName + ';

    --ALTER TABLE ' + @targetSchemaAndName + ' ALTER COLUMN <nullable_column> <type> NOT NULL;

    ALTER TABLE ' + @targetSchemaAndName + ' ADD CONSTRAINT PK_' + @schemaName + '_' + @tableName + ' PRIMARY KEY CLUSTERED (' + @updatedatetimeColumn + ', ' + REPLACE(@PKColumnsList, ',', ', ') + ');

    CREATE UNIQUE NONCLUSTERED INDEX IX_' + @schemaName + '_' + @tableName + '_BusinessKey ON ' + @targetSchemaAndName + ' (' + REPLACE(@PKColumnsList, ',', ', ') + ');
    --CREATE UNIQUE NONCLUSTERED INDEX IX_' + @schemaName + '_' + @tableName + '_AlternateKey ON ' + @targetSchemaAndName + ' ();
END;
GO

CREATE OR ALTER PROCEDURE ' + @mergeSPSchemaAndName + '
AS
BEGIN
    SET NOCOUNT ON;

    MERGE INTO ' + @targetSchemaAndName + ' AS TGT
    USING ' + @SchemaAndName + ' AS SRC ON (
        ' + @joinCondition + '
    )

    WHEN MATCHED AND SRC.' + @changeHashKeyColumn + ' <> TGT.' + @changeHashKeyColumn + '
      THEN UPDATE SET ' + CASE WHEN LEN(@nonPKColumnsListForUpdate) = 0 THEN LEFT(REPLACE(@DWHColumnsListForUpdate, ',', ', '), LEN(REPLACE(@DWHColumnsListForUpdate, ',', ', ')) - 1) ELSE + REPLACE(@DWHColumnsListForUpdate, ',', ', ') + '
        ' + REPLACE(@nonPKColumnsListForUpdate, ',', ', ') END + '

    WHEN NOT MATCHED BY TARGET
      THEN INSERT (' + REPLACE(@allColumnsList, ',', ', ') + ')
        VALUES (' + REPLACE(@allColumnsList, ',', ', ') + ')

    WHEN NOT MATCHED BY SOURCE AND TGT.' + @isDeletedColumn + ' = CAST(0 AS BIT)
      THEN UPDATE SET TGT.' + @changeHashKeyColumn + ' = CONVERT(VARBINARY(32), 0),
        TGT.' + @updatedatetimeColumn + ' = CURRENT_TIMESTAMP,
        TGT.' + @isDeletedColumn + ' = CAST(1 AS BIT)
    
    OUTPUT
        CURRENT_TIMESTAMP AS merge_datetime,
        CASE WHEN Inserted.' + @isDeletedColumn + ' = CAST(1 AS BIT) THEN N''DELETE'' ELSE $action END AS merge_action,
        ''' + @targetSchemaAndName + ''' AS full_olap_table_name,
       ' + @PKColumnsListForLog + ' AS primary_key_description
    INTO audit.merge_log_details;

END
GO

EXEC ' + @mergeSPSchemaAndName + ';
GO
';

    SELECT @SQLScript AS GeneratedSyncScript;

    DROP TABLE #viewColumns;

    DROP TABLE IF EXISTS #DWHColumns;

END;

GO

