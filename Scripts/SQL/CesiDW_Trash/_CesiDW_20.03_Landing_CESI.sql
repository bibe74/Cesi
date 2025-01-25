USE CesiDW;
GO

/*
SET NOEXEC OFF;
--*/ SET NOEXEC ON;
GO

/*
    SCHEMA_NAME > CESI
    TABLE_NAME > Users
*/

/**
 * @table Landing.CESI_Users
 * @description 

 * @depends CESI.Users

SELECT TOP 100 * FROM CESI.Users;
*/

IF OBJECT_ID('Landing.CESI_UsersView', 'V') IS NULL EXEC('CREATE VIEW Landing.CESI_UsersView AS SELECT 1 AS fld;');
GO

ALTER VIEW Landing.CESI_UsersView
AS
WITH TableData
AS (
    SELECT
        UserId,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            UserId,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            UserName,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
        UserName

    FROM CESI.Users
)
SELECT
    -- Chiavi
    TD.UserId,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Altri campi
    TD.UserName

FROM TableData TD;
GO

--DROP TABLE IF EXISTS Landing.CESI_Users;
GO

IF OBJECT_ID(N'Landing.CESI_Users', N'U') IS NULL
BEGIN
    SELECT TOP 0 * INTO Landing.CESI_Users FROM Landing.CESI_UsersView;

    ALTER TABLE Landing.CESI_Users ADD CONSTRAINT PK_Landing_CESI_Users PRIMARY KEY CLUSTERED (UpdateDatetime, UserId);

    ALTER TABLE Landing.CESI_Users ALTER COLUMN UserName NVARCHAR(60) NOT NULL;

    CREATE UNIQUE NONCLUSTERED INDEX IX_CESI_Users_BusinessKey ON Landing.CESI_Users (UserId);
END;
GO

IF OBJECT_ID('CESI.usp_Merge_Users', 'P') IS NULL EXEC('CREATE PROCEDURE CESI.usp_Merge_Users AS RETURN 0;');
GO

ALTER PROCEDURE CESI.usp_Merge_Users
AS
BEGIN
    SET NOCOUNT ON;

    MERGE INTO Landing.CESI_Users AS TGT
    USING Landing.CESI_UsersView (nolock) AS SRC
    ON SRC.UserId = TGT.UserId

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.UserName = SRC.UserName

    WHEN NOT MATCHED AND SRC.IsDeleted = CAST(0 AS BIT)
      THEN INSERT VALUES (
        UserId,

        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
    
        UserName
      )

    WHEN NOT MATCHED BY SOURCE
        AND TGT.IsDeleted = CAST(0 AS BIT)
      THEN UPDATE
        SET TGT.IsDeleted = CAST(1 AS BIT),
        TGT.UpdateDatetime = CURRENT_TIMESTAMP,
        TGT.ChangeHashKey = CONVERT(VARBINARY(20), ''),
        TGT.ChangeHashKeyASCII = ''

    OUTPUT
        CURRENT_TIMESTAMP AS merge_datetime,
        CASE WHEN Inserted.IsDeleted = CAST(1 AS BIT) THEN N'DELETE' ELSE $action END AS merge_action,
        'Landing.CESI_Users' AS full_olap_table_name,
        'UserId = ' + CAST(COALESCE(inserted.UserId, deleted.UserId) AS NVARCHAR(50)) AS primary_key_description
    INTO audit.merge_log_details;

END;
GO

EXEC CESI.usp_Merge_Users;
GO

/*
    SCHEMA_NAME > CESI
    TABLE_NAME > Membership
*/

/**
 * @table Landing.CESI_Membership
 * @description 

 * @depends CESI.Membership

SELECT TOP 100 * FROM CESI.Membership;
*/

IF OBJECT_ID('Landing.CESI_MembershipView', 'V') IS NULL EXEC('CREATE VIEW Landing.CESI_MembershipView AS SELECT 1 AS fld;');
GO

ALTER VIEW Landing.CESI_MembershipView
AS
WITH TableData
AS (
    SELECT
        UserId,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            UserId,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            LoweredEmail,
            IsApproved,
            IsLockedOut,
            CreateDate,
            LastLoginDate,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
        LoweredEmail AS Email,
        IsApproved,
        IsLockedOut,
        CreateDate,
        LastLoginDate

    FROM CESI.Membership
)
SELECT
    -- Chiavi
    TD.UserId,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Altri campi
    TD.Email,
    TD.IsApproved,
    TD.IsLockedOut,
    TD.CreateDate,
    TD.LastLoginDate

FROM TableData TD;
GO

--DROP TABLE IF EXISTS Landing.CESI_Membership;
GO

IF OBJECT_ID(N'Landing.CESI_Membership', N'U') IS NULL
BEGIN
    SELECT TOP 0 * INTO Landing.CESI_Membership FROM Landing.CESI_MembershipView;

    ALTER TABLE Landing.CESI_Membership ADD CONSTRAINT PK_Landing_CESI_Membership PRIMARY KEY CLUSTERED (UpdateDatetime, UserId);

    ALTER TABLE Landing.CESI_Membership ALTER COLUMN Email NVARCHAR(60) NOT NULL;

    CREATE UNIQUE NONCLUSTERED INDEX IX_CESI_Membership_BusinessKey ON Landing.CESI_Membership (UserId);
    CREATE NONCLUSTERED INDEX IX_CESI_Membership_Email ON Landing.CESI_Membership (Email);
END;
GO

IF OBJECT_ID('CESI.usp_Merge_Membership', 'P') IS NULL EXEC('CREATE PROCEDURE CESI.usp_Merge_Membership AS RETURN 0;');
GO

ALTER PROCEDURE CESI.usp_Merge_Membership
AS
BEGIN
    SET NOCOUNT ON;

    MERGE INTO Landing.CESI_Membership AS TGT
    USING Landing.CESI_MembershipView (nolock) AS SRC
    ON SRC.UserId = TGT.UserId

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.Email = SRC.Email,
        TGT.IsApproved = SRC.IsApproved,
        TGT.IsLockedOut = SRC.IsLockedOut,
        TGT.CreateDate = SRC.CreateDate,
        TGT.LastLoginDate = SRC.LastLoginDate

    WHEN NOT MATCHED AND SRC.IsDeleted = CAST(0 AS BIT)
      THEN INSERT VALUES (
        SRC.UserId,

        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
    
        Email,
        IsApproved,
        IsLockedOut,
        CreateDate,
        LastLoginDate
      )

    WHEN NOT MATCHED BY SOURCE
        AND TGT.IsDeleted = CAST(0 AS BIT)
      THEN UPDATE
        SET TGT.IsDeleted = CAST(1 AS BIT),
        TGT.UpdateDatetime = CURRENT_TIMESTAMP,
        TGT.ChangeHashKey = CONVERT(VARBINARY(20), ''),
        TGT.ChangeHashKeyASCII = ''

    OUTPUT
        CURRENT_TIMESTAMP AS merge_datetime,
        CASE WHEN Inserted.IsDeleted = CAST(1 AS BIT) THEN N'DELETE' ELSE $action END AS merge_action,
        'Landing.CESI_Membership' AS full_olap_table_name,
        'UserId = ' + CAST(COALESCE(inserted.UserId, deleted.UserId) AS NVARCHAR(50)) AS primary_key_description
    INTO audit.merge_log_details;

END;
GO

EXEC CESI.usp_Merge_Membership;
GO
