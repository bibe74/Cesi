
CREATE PROCEDURE audit.usp_refresh_all_views AS
BEGIN

	-- This sp will refresh all views in the catalog. 
	--     It enumerates all views, and runs sp_RefreshView for each of them

	DECLARE abc CURSOR FOR
	SELECT TABLE_SCHEMA + N'.' + TABLE_NAME AS ViewName
	FROM INFORMATION_SCHEMA.VIEWS
	ORDER BY TABLE_SCHEMA,
		TABLE_NAME;
	OPEN abc;

	DECLARE @ViewName varchar(261);

	-- Build select string
	DECLARE @SQLString nvarchar(2048);

	FETCH NEXT FROM abc INTO @ViewName;

	WHILE @@FETCH_STATUS = 0 
	BEGIN
		SET @SQLString = 'EXECUTE sp_RefreshView ''' + @ViewName + '''';
		--PRINT @SQLString

		BEGIN TRY
			EXECUTE sp_ExecuteSQL @SQLString;

			--PRINT 'OK ==> ' + @SQLString;
		END TRY
		BEGIN CATCH
			IF (@@TRANCOUNT > 0) ROLLBACK;

			PRINT 'KO ==> ' + @SQLString;
		END CATCH

		FETCH NEXT FROM abc INTO @ViewName;
	END;

	CLOSE abc;
	DEALLOCATE abc;

END;

GO

