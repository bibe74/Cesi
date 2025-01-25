USE CesiDW;
GO

/*
SET NOEXEC OFF;
--*/ SET NOEXEC ON;
GO

/*
    SCHEMA_NAME > MYSOLUTION
    TABLE_NAME > Address
*/

/**
 * @table Landing.MYSOLUTION_Address
 * @description 

 * @depends MYSOLUTION.Address

SELECT TOP 100 * FROM MYSOLUTION.Address;
*/

IF OBJECT_ID('Landing.MYSOLUTION_AddressView', 'V') IS NULL EXEC('CREATE VIEW Landing.MYSOLUTION_AddressView AS SELECT 1 AS fld;');
GO

ALTER VIEW Landing.MYSOLUTION_AddressView
AS
WITH TableData
AS (
    SELECT
        A.Id,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            A.Id,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            A.FirstName,
            A.LastName,
            A.Email,
            A.Company,
            C.Name,
            SP.Name,
            A.City,
            A.Address1,
            A.Address2,
            A.ZipPostalCode,
            A.PhoneNumber,
            A.County,
            A.CodiceFiscale,
            A.Piva,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
        COALESCE(A.FirstName, N'') AS FirstName,
        COALESCE(A.LastName, N'') AS LastName,
        COALESCE(A.Email, N'') AS Email,
        COALESCE(A.Company, N'') AS Company,
        --A.CountryId,
        COALESCE(C.Name, N'') AS Country,
        --A.StateProvinceId,
        COALESCE(SP.Name, N'') AS StateProvince,
        COALESCE(A.City, N'') AS City,
        COALESCE(A.Address1, N'') AS Address1,
        COALESCE(A.Address2, N'') AS Address2,
        COALESCE(A.ZipPostalCode, N'') AS ZipPostalCode,
        COALESCE(A.PhoneNumber, N'') AS PhoneNumber,
        COALESCE(A.County, N'') AS County,
        COALESCE(A.CodiceFiscale, N'') AS CodiceFiscale,
        COALESCE(A.Piva, N'') AS Piva

    FROM MYSOLUTION.Address A
    LEFT JOIN MYSOLUTIONPRODUZIONE2.Nop_MySolution.dbo.Country C ON C.Id = A.CountryId
    LEFT JOIN MYSOLUTIONPRODUZIONE2.Nop_MySolution.dbo.StateProvince SP ON SP.Id = A.StateProvinceId
)
SELECT
    -- Chiavi
    TD.Id,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Attributi
    TD.FirstName,
    TD.LastName,
    TD.Email,
    TD.Company,
    TD.Country,
    TD.StateProvince,
    TD.City,
    TD.Address1,
    TD.Address2,
    TD.ZipPostalCode,
    TD.PhoneNumber,
    TD.County,
    TD.CodiceFiscale,
    TD.Piva

FROM TableData TD;
GO

--DROP TABLE IF EXISTS Landing.MYSOLUTION_Address;
GO

IF OBJECT_ID(N'Landing.MYSOLUTION_Address', N'U') IS NULL
BEGIN
    SELECT TOP 0 * INTO Landing.MYSOLUTION_Address FROM Landing.MYSOLUTION_AddressView;

    ALTER TABLE Landing.MYSOLUTION_Address ADD CONSTRAINT PK_Landing_MYSOLUTION_Address PRIMARY KEY CLUSTERED (UpdateDatetime, Id);

    --ALTER TABLE Landing.MYSOLUTION_Address ALTER COLUMN  NVARCHAR(60) NOT NULL;

    CREATE UNIQUE NONCLUSTERED INDEX IX_MYSOLUTION_Address_BusinessKey ON Landing.MYSOLUTION_Address (Id);
END;
GO

IF OBJECT_ID('MYSOLUTION.usp_Merge_Address', 'P') IS NULL EXEC('CREATE PROCEDURE MYSOLUTION.usp_Merge_Address AS RETURN 0;');
GO

ALTER PROCEDURE MYSOLUTION.usp_Merge_Address
AS
BEGIN
    SET NOCOUNT ON;

    MERGE INTO Landing.MYSOLUTION_Address AS TGT
    USING Landing.MYSOLUTION_AddressView (nolock) AS SRC
    ON SRC.Id = TGT.Id

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.FirstName = SRC.FirstName,
        TGT.LastName = SRC.LastName,
        TGT.Email = SRC.Email,
        TGT.Company = SRC.Company,
        TGT.Country = SRC.Country,
        TGT.StateProvince = SRC.StateProvince,
        TGT.City = SRC.City,
        TGT.Address1 = SRC.Address1,
        TGT.Address2 = SRC.Address2,
        TGT.ZipPostalCode = SRC.ZipPostalCode,
        TGT.PhoneNumber = SRC.PhoneNumber,
        TGT.County = SRC.County,
        TGT.CodiceFiscale = SRC.CodiceFiscale,
        TGT.Piva = SRC.Piva

    WHEN NOT MATCHED AND SRC.IsDeleted = CAST(0 AS BIT)
      THEN INSERT VALUES (
        Id,

        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
    
        FirstName,
        LastName,
        Email,
        Company,
        Country,
        StateProvince,
        City,
        Address1,
        Address2,
        ZipPostalCode,
        PhoneNumber,
        County,
        CodiceFiscale,
        Piva
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
        'Landing.MYSOLUTION_Address' AS full_olap_table_name,
        'Id = ' + CAST(COALESCE(inserted.Id, deleted.Id) AS NVARCHAR) AS primary_key_description
    INTO audit.merge_log_details;

END;
GO

EXEC MYSOLUTION.usp_Merge_Address;
GO

/*
    SCHEMA_NAME > MYSOLUTION
    TABLE_NAME > Country
*/

/**
 * @table Landing.MYSOLUTION_Country
 * @description 

 * @depends MYSOLUTION.Country

SELECT TOP 100 * FROM MYSOLUTION.Country;
*/

IF OBJECT_ID('Landing.MYSOLUTION_CountryView', 'V') IS NULL EXEC('CREATE VIEW Landing.MYSOLUTION_CountryView AS SELECT 1 AS fld;');
GO

ALTER VIEW Landing.MYSOLUTION_CountryView
AS
WITH TableData
AS (
    SELECT
        Id,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            Id,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            Name,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
        Name

    FROM MYSOLUTION.Country
    WHERE Published = CAST(1 AS BIT)
)
SELECT
    -- Chiavi
    TD.Id,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Attributi
    TD.Name

FROM TableData TD;
GO

--DROP TABLE IF EXISTS Landing.MYSOLUTION_Country;
GO

IF OBJECT_ID(N'Landing.MYSOLUTION_Country', N'U') IS NULL
BEGIN
    SELECT TOP 0 * INTO Landing.MYSOLUTION_Country FROM Landing.MYSOLUTION_CountryView;

    ALTER TABLE Landing.MYSOLUTION_Country ADD CONSTRAINT PK_Landing_MYSOLUTION_Country PRIMARY KEY CLUSTERED (UpdateDatetime, Id);

    --ALTER TABLE Landing.MYSOLUTION_Country ALTER COLUMN  NVARCHAR(60) NOT NULL;

    CREATE UNIQUE NONCLUSTERED INDEX IX_MYSOLUTION_Country_BusinessKey ON Landing.MYSOLUTION_Country (Id);
END;
GO

IF OBJECT_ID('MYSOLUTION.usp_Merge_Country', 'P') IS NULL EXEC('CREATE PROCEDURE MYSOLUTION.usp_Merge_Country AS RETURN 0;');
GO

ALTER PROCEDURE MYSOLUTION.usp_Merge_Country
AS
BEGIN
    SET NOCOUNT ON;

    MERGE INTO Landing.MYSOLUTION_Country AS TGT
    USING Landing.MYSOLUTION_CountryView (nolock) AS SRC
    ON SRC.Id = TGT.Id

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.Name = SRC.Name

    WHEN NOT MATCHED AND SRC.IsDeleted = CAST(0 AS BIT)
      THEN INSERT VALUES (
        Id,

        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
    
        Name
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
        'Landing.MYSOLUTION_Country' AS full_olap_table_name,
        'Id = ' + CAST(COALESCE(inserted.Id, deleted.Id) AS NVARCHAR) AS primary_key_description
    INTO audit.merge_log_details;

END;
GO

EXEC MYSOLUTION.usp_Merge_Country;
GO

/*
    SCHEMA_NAME > MYSOLUTION
    TABLE_NAME > Customer
*/

/**
 * @table Landing.MYSOLUTION_Customer
 * @description 

 * @depends MYSOLUTION.Customer

SELECT TOP 100 * FROM MYSOLUTION.Customer;
*/

IF OBJECT_ID('Landing.MYSOLUTION_CustomerView', 'V') IS NULL EXEC('CREATE VIEW Landing.MYSOLUTION_CustomerView AS SELECT 1 AS fld;');
GO

ALTER VIEW Landing.MYSOLUTION_CustomerView
AS
WITH TableData
AS (
    SELECT
        Id,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            Id,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            Username,
            Email,
            IdCometa,
            AdminComment,
            IsTaxExempt,
            HasShoppingCartItems,
            Active,
            Deleted,
            IsSystemAccount,
            SystemName,
            LastIpAddress,
            CreatedOnUtc,
            LastLoginDateUtc,
            LastActivityDateUtc,
            CustomerGuid,
            EmailToRevalidate,
            AffiliateId,
            VendorId,
            RequireReLogin,
            FailedLoginAttempts,
            CannotLoginUntilDateUtc,
            RegisteredInStoreId,
            BillingAddress_Id,
            ShippingAddress_Id,
            MysolutionSubscriptionQuote,
            SendRiqualification,
            IsSpecial,
            DateExpiration,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
        Username,
        Email,
        IdCometa,
        AdminComment,
        IsTaxExempt,
        HasShoppingCartItems,
        Active,
        Deleted,
        IsSystemAccount,
        SystemName,
        LastIpAddress,
        CreatedOnUtc,
        LastLoginDateUtc,
        LastActivityDateUtc,
        CustomerGuid,
        EmailToRevalidate,
        AffiliateId,
        VendorId,
        RequireReLogin,
        FailedLoginAttempts,
        CannotLoginUntilDateUtc,
        RegisteredInStoreId,
        BillingAddress_Id,
        ShippingAddress_Id,
        MysolutionSubscriptionQuote,
        SendRiqualification,
        IsSpecial,
        DateExpiration

    FROM MYSOLUTION.Customer
    WHERE Active = CAST(1 AS BIT)
        AND Deleted = CAST(0 AS BIT)
        AND COALESCE(Username, Email) IS NOT NULL
)
SELECT
    -- Chiavi
    TD.Id,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Attributi
    COALESCE(TD.Username, TD.Email) AS Username,
    COALESCE(TD.Username, TD.Email) AS Email,
    TD.IdCometa,
    TD.AdminComment,
    TD.IsTaxExempt,
    TD.HasShoppingCartItems,
    TD.Active,
    TD.Deleted,
    TD.IsSystemAccount,
    TD.SystemName,
    TD.LastIpAddress,
    TD.CreatedOnUtc,
    TD.LastLoginDateUtc,
    TD.LastActivityDateUtc,
    TD.CustomerGuid,
    TD.EmailToRevalidate,
    TD.AffiliateId,
    TD.VendorId,
    TD.RequireReLogin,
    TD.FailedLoginAttempts,
    TD.CannotLoginUntilDateUtc,
    TD.RegisteredInStoreId,
    TD.BillingAddress_Id,
    TD.ShippingAddress_Id,
    TD.MysolutionSubscriptionQuote,
    TD.SendRiqualification,
    TD.IsSpecial,
    TD.DateExpiration

FROM TableData TD;
GO

--DROP TABLE IF EXISTS Landing.MYSOLUTION_Customer;
GO

IF OBJECT_ID(N'Landing.MYSOLUTION_Customer', N'U') IS NULL
BEGIN
    SELECT TOP 0 * INTO Landing.MYSOLUTION_Customer FROM Landing.MYSOLUTION_CustomerView;

    ALTER TABLE Landing.MYSOLUTION_Customer ADD CONSTRAINT PK_Landing_MYSOLUTION_Customer PRIMARY KEY CLUSTERED (UpdateDatetime, Id);

    --ALTER TABLE Landing.MYSOLUTION_Customer ALTER COLUMN  NVARCHAR(60) NOT NULL;

    CREATE UNIQUE NONCLUSTERED INDEX IX_MYSOLUTION_Customer_BusinessKey ON Landing.MYSOLUTION_Customer (Id);
END;
GO

IF OBJECT_ID('MYSOLUTION.usp_Merge_Customer', 'P') IS NULL EXEC('CREATE PROCEDURE MYSOLUTION.usp_Merge_Customer AS RETURN 0;');
GO

ALTER PROCEDURE MYSOLUTION.usp_Merge_Customer
AS
BEGIN
    SET NOCOUNT ON;

    MERGE INTO Landing.MYSOLUTION_Customer AS TGT
    USING Landing.MYSOLUTION_CustomerView (nolock) AS SRC
    ON SRC.Id = TGT.Id

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.Username = SRC.Username,
        TGT.Email = SRC.Email,
        TGT.IdCometa = SRC.IdCometa,
        TGT.AdminComment = SRC.AdminComment,
        TGT.IsTaxExempt = SRC.IsTaxExempt,
        TGT.HasShoppingCartItems = SRC.HasShoppingCartItems,
        TGT.Active = SRC.Active,
        TGT.Deleted = SRC.Deleted,
        TGT.IsSystemAccount = SRC.IsSystemAccount,
        TGT.SystemName = SRC.SystemName,
        TGT.LastIpAddress = SRC.LastIpAddress,
        TGT.CreatedOnUtc = SRC.CreatedOnUtc,
        TGT.LastLoginDateUtc = SRC.LastLoginDateUtc,
        TGT.LastActivityDateUtc = SRC.LastActivityDateUtc,
        TGT.CustomerGuid = SRC.CustomerGuid,
        TGT.EmailToRevalidate = SRC.EmailToRevalidate,
        TGT.AffiliateId = SRC.AffiliateId,
        TGT.VendorId = SRC.VendorId,
        TGT.RequireReLogin = SRC.RequireReLogin,
        TGT.FailedLoginAttempts = SRC.FailedLoginAttempts,
        TGT.CannotLoginUntilDateUtc = SRC.CannotLoginUntilDateUtc,
        TGT.RegisteredInStoreId = SRC.RegisteredInStoreId,
        TGT.BillingAddress_Id = SRC.BillingAddress_Id,
        TGT.ShippingAddress_Id = SRC.ShippingAddress_Id,
        TGT.MysolutionSubscriptionQuote = SRC.MysolutionSubscriptionQuote,
        TGT.SendRiqualification = SRC.SendRiqualification,
        TGT.IsSpecial = SRC.IsSpecial,
        TGT.DateExpiration = SRC.DateExpiration

    WHEN NOT MATCHED AND SRC.IsDeleted = CAST(0 AS BIT)
      THEN INSERT VALUES (
        Id,

        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
    
        Username,
        Email,
        IdCometa,
        AdminComment,
        IsTaxExempt,
        HasShoppingCartItems,
        Active,
        Deleted,
        IsSystemAccount,
        SystemName,
        LastIpAddress,
        CreatedOnUtc,
        LastLoginDateUtc,
        LastActivityDateUtc,
        CustomerGuid,
        EmailToRevalidate,
        AffiliateId,
        VendorId,
        RequireReLogin,
        FailedLoginAttempts,
        CannotLoginUntilDateUtc,
        RegisteredInStoreId,
        BillingAddress_Id,
        ShippingAddress_Id,
        MysolutionSubscriptionQuote,
        SendRiqualification,
        IsSpecial,
        DateExpiration
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
        'Landing.MYSOLUTION_Customer' AS full_olap_table_name,
        'Id = ' + CAST(COALESCE(inserted.Id, deleted.Id) AS NVARCHAR) AS primary_key_description
    INTO audit.merge_log_details;

END;
GO

EXEC MYSOLUTION.usp_Merge_Customer;
GO

/*
    SCHEMA_NAME > MYSOLUTION
    TABLE_NAME > Customer_CustomerRole_Mapping
*/

/**
 * @table Landing.MYSOLUTION_Customer_CustomerRole_Mapping
 * @description 

 * @depends MYSOLUTION.Customer_CustomerRole_Mapping

SELECT TOP 100 * FROM MYSOLUTION.Customer_CustomerRole_Mapping;
*/

IF OBJECT_ID('Landing.MYSOLUTION_Customer_CustomerRole_MappingView', 'V') IS NULL EXEC('CREATE VIEW Landing.MYSOLUTION_Customer_CustomerRole_MappingView AS SELECT 1 AS fld;');
GO

ALTER VIEW Landing.MYSOLUTION_Customer_CustomerRole_MappingView
AS
WITH TableData
AS (
    SELECT
        Customer_Id,
        CustomerRole_Id,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            Customer_Id,
            CustomerRole_Id,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            Customer_Id,
            CustomerRole_Id,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime

    FROM MYSOLUTION.Customer_CustomerRole_Mapping
)
SELECT
    -- Chiavi
    TD.Customer_Id,
    TD.CustomerRole_Id,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted

    -- Attributi

FROM TableData TD;
GO

--DROP TABLE IF EXISTS Landing.MYSOLUTION_Customer_CustomerRole_Mapping;
GO

IF OBJECT_ID(N'Landing.MYSOLUTION_Customer_CustomerRole_Mapping', N'U') IS NULL
BEGIN
    SELECT TOP 0 * INTO Landing.MYSOLUTION_Customer_CustomerRole_Mapping FROM Landing.MYSOLUTION_Customer_CustomerRole_MappingView;

    ALTER TABLE Landing.MYSOLUTION_Customer_CustomerRole_Mapping ADD CONSTRAINT PK_Landing_MYSOLUTION_Customer_CustomerRole_Mapping PRIMARY KEY CLUSTERED (UpdateDatetime, Customer_Id, CustomerRole_Id);

    --ALTER TABLE Landing.MYSOLUTION_Customer_CustomerRole_Mapping ALTER COLUMN  NVARCHAR(60) NOT NULL;

    CREATE UNIQUE NONCLUSTERED INDEX IX_MYSOLUTION_Customer_CustomerRole_Mapping_BusinessKey ON Landing.MYSOLUTION_Customer_CustomerRole_Mapping (Customer_Id, CustomerRole_Id);
END;
GO

IF OBJECT_ID('MYSOLUTION.usp_Merge_Customer_CustomerRole_Mapping', 'P') IS NULL EXEC('CREATE PROCEDURE MYSOLUTION.usp_Merge_Customer_CustomerRole_Mapping AS RETURN 0;');
GO

ALTER PROCEDURE MYSOLUTION.usp_Merge_Customer_CustomerRole_Mapping
AS
BEGIN
    SET NOCOUNT ON;

    MERGE INTO Landing.MYSOLUTION_Customer_CustomerRole_Mapping AS TGT
    USING Landing.MYSOLUTION_Customer_CustomerRole_MappingView (NOLOCK) AS SRC
    ON SRC.Customer_Id = TGT.Customer_Id AND SRC.CustomerRole_Id = TGT.CustomerRole_Id

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime

    WHEN NOT MATCHED AND SRC.IsDeleted = CAST(0 AS BIT)
      THEN INSERT VALUES (
        Customer_Id,
        CustomerRole_Id,

        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted
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
        'Landing.MYSOLUTION_Customer_CustomerRole_Mapping' AS full_olap_table_name,
        'Customer_Id = ' + CAST(COALESCE(inserted.Customer_Id, deleted.Customer_Id) AS NVARCHAR)
        + 'CustomerRole_Id = ' + CAST(COALESCE(inserted.CustomerRole_Id, deleted.CustomerRole_Id) AS NVARCHAR) AS primary_key_description
    INTO audit.merge_log_details;

END;
GO

EXEC MYSOLUTION.usp_Merge_Customer_CustomerRole_Mapping;
GO

/*
    SCHEMA_NAME > MYSOLUTION
    TABLE_NAME > CustomerAddresses
*/

/**
 * @table Landing.MYSOLUTION_CustomerAddresses
 * @description 

 * @depends MYSOLUTION.CustomerAddresses

SELECT TOP 100 * FROM MYSOLUTION.CustomerAddresses;
*/

IF OBJECT_ID('Landing.MYSOLUTION_CustomerAddressesView', 'V') IS NULL EXEC('CREATE VIEW Landing.MYSOLUTION_CustomerAddressesView AS SELECT 1 AS fld;');
GO

ALTER VIEW Landing.MYSOLUTION_CustomerAddressesView
AS
WITH TableData
AS (
    SELECT
        Customer_Id,
        Address_Id,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            Customer_Id,
            Address_Id,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            Customer_Id,
            Address_Id,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime

    FROM MYSOLUTION.CustomerAddresses
)
SELECT
    -- Chiavi
    TD.Customer_Id,
    TD.Address_Id,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted

    -- Attributi

FROM TableData TD;
GO

--DROP TABLE IF EXISTS Landing.MYSOLUTION_CustomerAddresses;
GO

IF OBJECT_ID(N'Landing.MYSOLUTION_CustomerAddresses', N'U') IS NULL
BEGIN
    SELECT TOP 0 * INTO Landing.MYSOLUTION_CustomerAddresses FROM Landing.MYSOLUTION_CustomerAddressesView;

    ALTER TABLE Landing.MYSOLUTION_CustomerAddresses ADD CONSTRAINT PK_Landing_MYSOLUTION_CustomerAddresses PRIMARY KEY CLUSTERED (UpdateDatetime, Customer_Id, Address_Id);

    --ALTER TABLE Landing.MYSOLUTION_CustomerAddresses ALTER COLUMN  NVARCHAR(60) NOT NULL;

    CREATE UNIQUE NONCLUSTERED INDEX IX_MYSOLUTION_CustomerAddresses_BusinessKey ON Landing.MYSOLUTION_CustomerAddresses (Customer_Id, Address_Id);
END;
GO

IF OBJECT_ID('MYSOLUTION.usp_Merge_CustomerAddresses', 'P') IS NULL EXEC('CREATE PROCEDURE MYSOLUTION.usp_Merge_CustomerAddresses AS RETURN 0;');
GO

ALTER PROCEDURE MYSOLUTION.usp_Merge_CustomerAddresses
AS
BEGIN
    SET NOCOUNT ON;

    MERGE INTO Landing.MYSOLUTION_CustomerAddresses AS TGT
    USING Landing.MYSOLUTION_CustomerAddressesView (nolock) AS SRC
    ON SRC.Customer_Id = TGT.Customer_Id AND SRC.Address_Id = TGT.Address_id

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime

    WHEN NOT MATCHED AND SRC.IsDeleted = CAST(0 AS BIT)
      THEN INSERT VALUES (
        Customer_Id,
        Address_Id,

        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted
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
        'Landing.MYSOLUTION_CustomerAddresses' AS full_olap_table_name,
        'Customer_Id = ' + CAST(COALESCE(inserted.Customer_Id, deleted.Customer_Id) AS NVARCHAR)
        + 'Address_Id = ' + CAST(COALESCE(inserted.Address_Id, deleted.Address_Id) AS NVARCHAR) AS primary_key_description
    INTO audit.merge_log_details;

END;
GO

EXEC MYSOLUTION.usp_Merge_CustomerAddresses;
GO

/*
    SCHEMA_NAME > MYSOLUTION
    TABLE_NAME > CustomerRole
*/

/**
 * @table Landing.MYSOLUTION_CustomerRole
 * @description 

 * @depends MYSOLUTION.CustomerRole

SELECT TOP 100 * FROM MYSOLUTION.CustomerRole;
*/

IF OBJECT_ID('Landing.MYSOLUTION_CustomerRoleView', 'V') IS NULL EXEC('CREATE VIEW Landing.MYSOLUTION_CustomerRoleView AS SELECT 1 AS fld;');
GO

ALTER VIEW Landing.MYSOLUTION_CustomerRoleView
AS
WITH TableData
AS (
    SELECT
        Id,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            Id,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            Name,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
        Name

    FROM MYSOLUTION.CustomerRole
)
SELECT
    -- Chiavi
    TD.Id,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Attributi
    Name

FROM TableData TD;
GO

--DROP TABLE IF EXISTS Landing.MYSOLUTION_CustomerRole;
GO

IF OBJECT_ID(N'Landing.MYSOLUTION_CustomerRole', N'U') IS NULL
BEGIN
    SELECT TOP 0 * INTO Landing.MYSOLUTION_CustomerRole FROM Landing.MYSOLUTION_CustomerRoleView;

    ALTER TABLE Landing.MYSOLUTION_CustomerRole ADD CONSTRAINT PK_Landing_MYSOLUTION_CustomerRole PRIMARY KEY CLUSTERED (UpdateDatetime, Id);

    --ALTER TABLE Landing.MYSOLUTION_CustomerRole ALTER COLUMN  NVARCHAR(60) NOT NULL;

    CREATE UNIQUE NONCLUSTERED INDEX IX_MYSOLUTION_CustomerRole_BusinessKey ON Landing.MYSOLUTION_CustomerRole (Id);
END;
GO

IF OBJECT_ID('MYSOLUTION.usp_Merge_CustomerRole', 'P') IS NULL EXEC('CREATE PROCEDURE MYSOLUTION.usp_Merge_CustomerRole AS RETURN 0;');
GO

ALTER PROCEDURE MYSOLUTION.usp_Merge_CustomerRole
AS
BEGIN
    SET NOCOUNT ON;

    MERGE INTO Landing.MYSOLUTION_CustomerRole AS TGT
    USING Landing.MYSOLUTION_CustomerRoleView (nolock) AS SRC
    ON SRC.Id = TGT.Id

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.Name = SRC.Name

    WHEN NOT MATCHED AND SRC.IsDeleted = CAST(0 AS BIT)
      THEN INSERT VALUES (
        Id,

        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,

        Name
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
        'Landing.MYSOLUTION_CustomerRole' AS full_olap_table_name,
        'Id = ' + CAST(COALESCE(inserted.Id, deleted.Id) AS NVARCHAR) AS primary_key_description
    INTO audit.merge_log_details;

END;
GO

EXEC MYSOLUTION.usp_Merge_CustomerRole;
GO

/*
    SCHEMA_NAME > MYSOLUTION
    TABLE_NAME > GenericAttribute
*/

/**
 * @table Landing.MYSOLUTION_GenericAttribute
 * @description 

 * @depends MYSOLUTION.GenericAttribute

SELECT TOP 100 * FROM MYSOLUTION.GenericAttribute;
*/

IF OBJECT_ID('Landing.MYSOLUTION_GenericAttributeView', 'V') IS NULL EXEC('CREATE VIEW Landing.MYSOLUTION_GenericAttributeView AS SELECT 1 AS fld;');
GO

ALTER VIEW Landing.MYSOLUTION_GenericAttributeView
AS
WITH TableData
AS (
    SELECT
        Id,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            Id,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            EntityId,
            [Key],
            Value,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
        EntityId,
        [Key],
        Value

    FROM MYSOLUTION.GenericAttribute
    WHERE [Key] IN
    (
        N'Cellulare',
        N'City',
        N'CodiceFiscale',
        N'Company',
        N'FirstName',
        N'LastName',
        N'Phone',
        N'StreetAddress',
        N'VatNumber',
        N'ZipPostalCode',
        N'CountryId',
        N'StateProvinceId'

        --N' ProfessionId',
        --N'AccountActivationToken',
        --N'AllSettingsPage.HideSearchBlock',
        --N'CatalogSettingsPage.HideTagsBlock',
        --N'category-advanced-mode',
        --N'CategoryPage.HideDisplayBlock',
        --N'CategoryPage.HideInfoBlock',
        --N'CategoryPage.HideMappingsBlock',
        --N'CategoryPage.HideProductsBlock',
        --N'CategoryPage.HideSEOBlock',
        --N'checkoutattribute-advanced-mode',
        --N'CodiceCIG',
        --N'CodiceIPA',
        --N'CodiceSDI',
        --N'CountryPage.HideStatesBlock',
        --N'CurrencyId',
        --N'CustomerListPage.HideSearchBlock',
        --N'CustomerPage.HideActivityLogBlock',
        --N'CustomerPage.HideAddressesBlock',
        --N'CustomerPage.HideBackInStockSubscriptionsBlock',
        --N'CustomerPage.HideInfoBlock',
        --N'CustomerPage.HideOrdersBlock',
        --N'CustomerPage.HidePlaceOrderBlock',
        --N'CustomerPage.HideShoppingCartAndWishlistBlock',
        --N'CustomerTypeId',
        --N'CustomerUserSettingsPage.HideAccountBlock',
        --N'CustomerUserSettingsPage.HideAddressFormFieldsBlock',
        --N'CustomerUserSettingsPage.HideCustomerFormFieldsBlock',
        --N'CustomerUserSettingsPage.HideProfileBlock',
        --N'CustomerUserSettingsPage.HideSecurityBlock',
        --N'DateOfBirth',
        --N'DiscountCouponCode',
        --N'DiscountPage.HideAppliedToProductsBlock',
        --N'DiscountPage.HideInfoBlock',
        --N'DiscountPage.HideRequirementsBlock',
        --N'DiscountPage.HideUsageHistoryBlock',
        --N'EmailPec',
        --N'EuCookieLaw.Accepted',
        --N'Gender',
        --N'GeneralCommonSettingsPage.HideAdminAreaBlock',
        --N'GiftCardCouponCodes',
        --N'GiftCardPage.HideUsageHistoryBlock',
        --N'HideCommonStatisticsPanel',
        --N'HideNopCommerceNewsPanel',
        --N'HideSidebar',
        --N'LanguageId',
        --N'LanguagePage.HideResourcesBlock',
        --N'LastContinueShoppingPage',
        --N'manufacturer-advanced-mode',
        --N'ManufacturersPage.HideSearchBlock',
        --N'messagetemplate-advanced-mode',
        --N'NewsItemPage.HideSeoBlock',
        --N'OfferedShippingOptions',
        --N'OrderPage.HideBillingAndShippingBlock',
        --N'OrderPage.HideInfoBlock',
        --N'OrderPage.HideProductsBlock',
        --N'OrderSettingsPage.HideGiftCardsBlock',
        --N'OrdersPage.HideSearchBlock',
        --N'PasswordRecoveryToken',
        --N'PasswordRecoveryTokenDateGenerated',
        --N'product-advanced-mode',
        --N'ProductAttributeMappingPage.HideCommonBlock',
        --N'ProductAttributeMappingPage.HideValuesBlock',
        --N'ProductAttributePage.HidePredefinedValuesBlock',
        --N'ProductAttributePage.HideUsedByProductsBlock',
        --N'ProductImageProportionCss-SevenSpikes.Theme.Prisma',
        --N'ProductListPage.HideSearchBlock',
        --N'ProductPage.HideCourseBlock',
        --N'ProductPage.HideCrossSellsProductsBlock',
        --N'ProductPage.HideDownloadableBlock',
        --N'ProductPage.HideGiftCardBlock',
        --N'ProductPage.HideInfoBlock',
        --N'ProductPage.HideInventoryBlock',
        --N'ProductPage.HidePicturesBlock',
        --N'ProductPage.HidePricesBlock',
        --N'ProductPage.HideProductAttributesBlock',
        --N'ProductPage.HidePurchasedWithOrdersBlock',
        --N'ProductPage.HideRecurringBlock',
        --N'ProductPage.HideRelatedProductsBlock',
        --N'ProductPage.HideRentalBlock',
        --N'ProductPage.HideSEOBlock',
        --N'ProductPage.HideShippingBlock',
        --N'ProductPage.HideSpecificationAttributeBlock',
        --N'ProductPage.HideStockQuantityHistoryBlock',
        --N'ProductPage.SS.Attachments',
        --N'ProfessionDetailId',
        --N'QueuedEmailsPage.HideSearchBlock',
        --N'Reports.HideBestsellersBriefReportByAmountPanel',
        --N'Reports.HideBestsellersBriefReportByQuantityPanel',
        --N'Reports.HideCustomerStatisticsPanel',
        --N'Reports.HideLatestOrdersPanel',
        --N'Reports.HideOrderAverageReportPanel',
        --N'Reports.HideOrderIncompleteReportPanel',
        --N'Reports.HideOrderStatisticsPanel',
        --N'Reports.HidePopularSearchTermsReport',
        --N'SelectedPaymentMethod',
        --N'SelectedShippingOption',
        --N'settings-advanced-mode',
        --N'ShoppingCartSettingsPage.HideCommonBlock',
        --N'ShoppingCartSettingsPage.HideMiniShoppingCartBlock',
        --N'SpecificationAttributePage.HideInfoBlock',
        --N'SpecificationAttributePage.HideOptionsBlock',
        --N'SpecificationAttributePage.HideUsedByProductsBlock',
        --N'SplitPayment',
        --N'store-advanced-mode',
        --N'StreetAddress2',
        --N'topic-advanced-mode',
        --N'TopicDetailsPage.HideDisplayBlock',
        --N'TopicDetailsPage.HideInfoBlock',
        --N'TopicDetailsPage.HideSeoBlock',
        --N'TopicsPage.HideSearchBlock',
        --N'UseRewardPointsDuringCheckout',
        --N'VatNumberStatusId',
        --N'vendor-advanced-mode',
    )
)
SELECT
    -- Chiavi
    TD.Id,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Attributi
    TD.EntityId,
    TD.[Key],
    TD.Value

FROM TableData TD;
GO

--DROP TABLE IF EXISTS Landing.MYSOLUTION_GenericAttribute;
GO

IF OBJECT_ID(N'Landing.MYSOLUTION_GenericAttribute', N'U') IS NULL
BEGIN
    SELECT TOP 0 * INTO Landing.MYSOLUTION_GenericAttribute FROM Landing.MYSOLUTION_GenericAttributeView;

    ALTER TABLE Landing.MYSOLUTION_GenericAttribute ADD CONSTRAINT PK_Landing_MYSOLUTION_GenericAttribute PRIMARY KEY CLUSTERED (UpdateDatetime, Id);

    --ALTER TABLE Landing.MYSOLUTION_GenericAttribute ALTER COLUMN  NVARCHAR(60) NOT NULL;

    CREATE UNIQUE NONCLUSTERED INDEX IX_MYSOLUTION_GenericAttribute_BusinessKey ON Landing.MYSOLUTION_GenericAttribute (Id);
END;
GO

IF OBJECT_ID('MYSOLUTION.usp_Merge_GenericAttribute', 'P') IS NULL EXEC('CREATE PROCEDURE MYSOLUTION.usp_Merge_GenericAttribute AS RETURN 0;');
GO

ALTER PROCEDURE MYSOLUTION.usp_Merge_GenericAttribute
AS
BEGIN
    SET NOCOUNT ON;

    MERGE INTO Landing.MYSOLUTION_GenericAttribute AS TGT
    USING Landing.MYSOLUTION_GenericAttributeView (nolock) AS SRC
    ON SRC.Id = TGT.Id

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.EntityId = SRC.EntityId,
        TGT.[Key] = SRC.[Key],
        TGT.Value = SRC.Value

    WHEN NOT MATCHED AND SRC.IsDeleted = CAST(0 AS BIT)
      THEN INSERT VALUES (
        Id,

        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
    
        EntityId,
        [Key],
        Value
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
        'Landing.MYSOLUTION_GenericAttribute' AS full_olap_table_name,
        'Id = ' + CAST(COALESCE(inserted.Id, deleted.Id) AS NVARCHAR) AS primary_key_description
    INTO audit.merge_log_details;

END;
GO

EXEC MYSOLUTION.usp_Merge_GenericAttribute;
GO

/*
    SCHEMA_NAME > MYSOLUTION
    TABLE_NAME > LogsForReport
*/

/**
 * @table Landing.MYSOLUTION_LogsForReport
 * @description 

 * @depends MYSOLUTION.LogsEpiServer

SELECT TOP 100 * FROM MYSOLUTION.LogsEpiServer;
*/

IF OBJECT_ID('Landing.MYSOLUTION_LogsForReportView', 'V') IS NULL EXEC('CREATE VIEW Landing.MYSOLUTION_LogsForReportView AS SELECT 1 AS fld;');
GO

ALTER VIEW Landing.MYSOLUTION_LogsForReportView
AS
WITH AggregatedData
AS (
    SELECT
        CAST(DataOra AS DATE) AS Data,
        Username,
        SUM(CASE WHEN PageType = N'Login' THEN 1 ELSE 0 END) AS NumeroAccessi,
        COUNT(1) AS NumeroPagineVisitate

    FROM MYSOLUTION.LogsEpiServer
    WHERE COALESCE(Username, N'') <> N''
    GROUP BY CAST(DataOra AS DATE),
        Username
),
TableData
AS (
    SELECT
        AD.Data,
        AD.Username,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            AD.Data,
            AD.Username,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            AD.NumeroAccessi,
            AD.NumeroPagineVisitate,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
        AD.NumeroAccessi,
        AD.NumeroPagineVisitate

    FROM AggregatedData AD
    WHERE AD.Data > CAST('19000101' AS DATE)
)
SELECT
    -- Chiavi
    TD.Data, -- PKData
    TD.Username, -- Username

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Altri campi
    TD.NumeroAccessi,
    TD.NumeroPagineVisitate

FROM TableData TD;
GO

--DROP TABLE IF EXISTS Landing.MYSOLUTION_LogsForReport;
GO

IF OBJECT_ID(N'Landing.MYSOLUTION_LogsForReport', N'U') IS NULL
BEGIN
    SELECT TOP 0 * INTO Landing.MYSOLUTION_LogsForReport FROM Landing.MYSOLUTION_LogsForReportView;

    ALTER TABLE Landing.MYSOLUTION_LogsForReport ALTER COLUMN Data DATE NOT NULL;
    ALTER TABLE Landing.MYSOLUTION_LogsForReport ALTER COLUMN Username NVARCHAR(50) NOT NULL;

    ALTER TABLE Landing.MYSOLUTION_LogsForReport ADD CONSTRAINT PK_Landing_MYSOLUTION_LogsForReport PRIMARY KEY CLUSTERED (UpdateDatetime, Data, Username);

    CREATE UNIQUE NONCLUSTERED INDEX IX_MYSOLUTION_LogsForReport_BusinessKey ON Landing.MYSOLUTION_LogsForReport (Data, Username);
END;
GO

IF OBJECT_ID('MYSOLUTION.usp_Merge_LogsForReport', 'P') IS NULL EXEC('CREATE PROCEDURE MYSOLUTION.usp_Merge_LogsForReport AS RETURN 0;');
GO

ALTER PROCEDURE MYSOLUTION.usp_Merge_LogsForReport
AS
BEGIN
    SET NOCOUNT ON;

    MERGE INTO Landing.MYSOLUTION_LogsForReport AS TGT
    USING Landing.MYSOLUTION_LogsForReportView (nolock) AS SRC
    ON SRC.Data = TGT.Data AND SRC.Username = TGT.Username

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.NumeroAccessi = SRC.NumeroAccessi,
        TGT.NumeroPagineVisitate = SRC.NumeroPagineVisitate

    WHEN NOT MATCHED AND SRC.IsDeleted = CAST(0 AS BIT)
      THEN INSERT VALUES (
        Data,
        Username,

        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
    
        NumeroAccessi,
        NumeroPagineVisitate
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
        'Landing.MYSOLUTION_LogsForReport' AS full_olap_table_name,
        'Data/Username = ' + CAST(COALESCE(inserted.Data, deleted.Data) AS NVARCHAR) + N'/'+ CAST(COALESCE(inserted.Username, deleted.Username) AS NVARCHAR(50)) AS primary_key_description
    INTO audit.merge_log_details;

END;
GO

EXEC MYSOLUTION.usp_Merge_LogsForReport;
GO

/*
    SCHEMA_NAME > MYSOLUTION
    TABLE_NAME > OrderItemParticipant
*/

/**
 * @table Landing.MYSOLUTION_OrderItemPartecipant
 * @description 

 * @depends MYSOLUTION.[Order]
 * @depends MYSOLUTION.OrderItem
 * @depends MYSOLUTION.OrderItem_Partecipants
 * @depends MYSOLUTION.Partecipant
 * @depends MYSOLUTION.Product
 * @depends MYSOLUTION.ProductAttributeCombination

SELECT TOP (1) * FROM MYSOLUTION.[Order];
SELECT TOP (1) * FROM MYSOLUTION.OrderItem;
SELECT TOP (1) * FROM MYSOLUTION.OrderItem_Partecipants;
SELECT TOP (1) * FROM MYSOLUTION.Partecipant;
SELECT TOP (1) * FROM MYSOLUTION.Product;
SELECT TOP (1) * FROM MYSOLUTION.ProductAttributeCombination;
*/

IF OBJECT_ID('Landing.MYSOLUTION_OrderItemPartecipantView', 'V') IS NULL EXEC('CREATE VIEW Landing.MYSOLUTION_OrderItemPartecipantView AS SELECT 1 AS fld;');
GO

ALTER VIEW Landing.MYSOLUTION_OrderItemPartecipantView
AS
WITH TableData
AS (
    SELECT

        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            OI.Id,
            COALESCE(PA.Id, 0),
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            O.Id,
            O.OrderTotal,
            O.AuthorizationTransactionId,
            O.PaidDateUtc,
            O.CreatedOnUtc,
            O.CustomerId,
            O.OrderStatusId,
            O.CustomOrderNumber,
            OI.UnitPriceExclTax,
            OI.AttributeDescription,
            (CAST(OI.[AttributesXml] AS XML).value ('(/Attributes/ProductAttribute/@ID)[1]', 'varchar(max)')),
            (CAST(OI.[AttributesXml] AS XML).value ('(/Attributes/ProductAttribute/ProductAttributeValue/Value)[1]', 'varchar(max)')),
            (CAST(OI.[AttributesXml] AS XML).value ('(/Attributes/ProductAttribute/@ID)[2]', 'varchar(max)')),
            (CAST(OI.[AttributesXml] AS XML).value ('(/Attributes/ProductAttribute/ProductAttributeValue/Value)[2]', 'varchar(max)')),
            P.Id,
            P.Name,
            P.ShortDescription,
            P.Sku,
            P.Gtin,
            P.Subdescription,
            PAC.Sku,
            PAC.Gtin,
            PA.FirstName,
            PA.LastName,
            PA.Email,
            PA.Ssn,
            PARoot.Email,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,

        O.Id AS OrderId,
        O.OrderTotal,
        O.AuthorizationTransactionId AS OrderAuthorizationTransactionId,
        O.PaidDateUtc AS OrderPaidDate,
        O.CreatedOnUtc AS OrderCreatedDate,
        O.CustomerId AS OrderCustomerId,
        O.OrderStatusId,
        O.CustomOrderNumber AS OrderNumber,

        OI.Id AS OrderItemId,
        OI.UnitPriceExclTax AS OrderItemUnitPriceExclTax,
        OI.AttributeDescription AS OrderItemAttributeDescription,
        (CAST(OI.[AttributesXml] AS XML).value ('(/Attributes/ProductAttribute/@ID)[1]', 'varchar(max)')) AS AttributeMappingId,
        (CAST(OI.[AttributesXml] AS XML).value ('(/Attributes/ProductAttribute/ProductAttributeValue/Value)[1]', 'varchar(max)')) AS AttributeValue,
        (CAST(OI.[AttributesXml] AS XML).value ('(/Attributes/ProductAttribute/@ID)[2]', 'varchar(max)')) AS AttributeMappingId2,
        (CAST(OI.[AttributesXml] AS XML).value ('(/Attributes/ProductAttribute/ProductAttributeValue/Value)[2]', 'varchar(max)')) AS AttributeValue2,

        P.Id AS ProductId,
        P.Name AS ProductName,
        P.ShortDescription AS ProductShortDescription,
        P.Sku AS ProductSku,
        P.Gtin AS ProductGtin,
        P.Subdescription AS ProductSubdescription,

        PAC.Sku AS ProductAttributeCombinationSku,
        PAC.Gtin AS ProductAttributeCombinationGtin,

        COALESCE(PA.Id, 0) AS PartecipantId,
        PA.FirstName AS PartecipantFirstName,
        PA.LastName AS PartecipantLastName,
        PA.Email AS PartecipantEmail,
        PA.Ssn AS PartecipantFiscalCode,
        PARoot.Email AS RootPartecipantEmail

    FROM MYSOLUTION.[Order] O
    INNER JOIN Landing.MYSOLUTION_Customer C ON C.Id = O.CustomerId
    INNER JOIN MYSOLUTION.OrderItem OI ON OI.OrderId = O.Id
    INNER JOIN MYSOLUTION.Product P ON P.Id = OI.ProductId
        AND P.Deleted = 0
    LEFT JOIN MYSOLUTION.ProductAttributeCombination PAC ON PAC.ProductId = OI.ProductId AND PAC.AttributesXml = OI.AttributesXml
    LEFT JOIN MYSOLUTION.OrderItem_Partecipants OIP ON OIP.OrderItem_Id = OI.Id
    LEFT JOIN MYSOLUTION.Partecipant PA ON PA.Id = OIP.Partecipant_Id
    LEFT JOIN MYSOLUTION.Partecipant PARoot ON PARoot.Id = PA.OriginalPartecipantId
    WHERE O.OrderStatusId = 30 -- pagato
        AND O.Deleted = 0

)
SELECT
    -- Chiavi
    TD.OrderItemId,
    TD.PartecipantId,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Attributi
    TD.OrderId,
    TD.OrderTotal,
    TD.OrderAuthorizationTransactionId,
    TD.OrderPaidDate,
    TD.OrderCreatedDate,
    TD.OrderCustomerId,
    TD.OrderStatusId,
    TD.OrderNumber,
    TD.OrderItemUnitPriceExclTax,
    TD.OrderItemAttributeDescription,
    TD.AttributeMappingId,
    TD.AttributeValue,
    TD.AttributeMappingId2,
    TD.AttributeValue2,
    TD.ProductId,
    TD.ProductName, -- CourseName
    TD.ProductShortDescription, -- CourseType
    TD.ProductSku, -- CourseCode
    TD.ProductGtin, -- WebinarCode
    TD.ProductSubdescription, -- StartDateDescription (dal dd/mm/yyyy)
    TD.ProductAttributeCombinationSku, -- AttCourseCode
    TD.ProductAttributeCombinationGtin, -- AttWebinarCode
    TD.PartecipantFirstName,
    TD.PartecipantLastName,
    TD.PartecipantEmail,
    TD.PartecipantFiscalCode,
    TD.RootPartecipantEmail

FROM TableData TD;
GO

--DROP TABLE IF EXISTS Landing.MYSOLUTION_OrderItemPartecipant;
GO

IF OBJECT_ID(N'Landing.MYSOLUTION_OrderItemPartecipant', N'U') IS NULL
BEGIN
    SELECT TOP 0 * INTO Landing.MYSOLUTION_OrderItemPartecipant FROM Landing.MYSOLUTION_OrderItemPartecipantView;

    ALTER TABLE Landing.MYSOLUTION_OrderItemPartecipant ADD CONSTRAINT PK_Landing_MYSOLUTION_OrderItemPartecipant PRIMARY KEY CLUSTERED (UpdateDatetime, OrderItemId, PartecipantId);

    --ALTER TABLE Landing.MYSOLUTION_OrderItemPartecipant ALTER COLUMN  NVARCHAR(60) NOT NULL;

    CREATE UNIQUE NONCLUSTERED INDEX IX_MYSOLUTION_OrderItemPartecipant_BusinessKey ON Landing.MYSOLUTION_OrderItemPartecipant (OrderItemId, PartecipantId);
END;
GO

IF OBJECT_ID('MYSOLUTION.usp_Merge_OrderItemPartecipant', 'P') IS NULL EXEC('CREATE PROCEDURE MYSOLUTION.usp_Merge_OrderItemPartecipant AS RETURN 0;');
GO

ALTER PROCEDURE MYSOLUTION.usp_Merge_OrderItemPartecipant
AS
BEGIN
    SET NOCOUNT ON;

    MERGE INTO Landing.MYSOLUTION_OrderItemPartecipant AS TGT
    USING Landing.MYSOLUTION_OrderItemPartecipantView (nolock) AS SRC
    ON SRC.OrderItemId = TGT.OrderItemId AND SRC.PartecipantId = TGT.PartecipantId

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.OrderId = SRC.OrderId,
        TGT.OrderTotal = SRC.OrderTotal,
        TGT.OrderAuthorizationTransactionId = SRC.OrderAuthorizationTransactionId,
        TGT.OrderPaidDate = SRC.OrderPaidDate,
        TGT.OrderCreatedDate = SRC.OrderCreatedDate,
        TGT.OrderCustomerId = SRC.OrderCustomerId,
        TGT.OrderStatusId = SRC.OrderStatusId,
        TGT.OrderNumber = SRC.OrderNumber,
        TGT.OrderItemUnitPriceExclTax = SRC.OrderItemUnitPriceExclTax,
        TGT.OrderItemAttributeDescription = SRC.OrderItemAttributeDescription,
        TGT.AttributeMappingId = SRC.AttributeMappingId,
        TGT.AttributeValue = SRC.AttributeValue,
        TGT.AttributeMappingId2 = SRC.AttributeMappingId2,
        TGT.AttributeValue2 = SRC.AttributeValue2,
        TGT.ProductId = SRC.ProductId,
        TGT.ProductName = SRC.ProductName,
        TGT.ProductShortDescription = SRC.ProductShortDescription,
        TGT.ProductSku = SRC.ProductSku,
        TGT.ProductGtin = SRC.ProductGtin,
        TGT.ProductSubdescription = SRC.ProductSubdescription,
        TGT.ProductAttributeCombinationSku = SRC.ProductAttributeCombinationSku,
        TGT.ProductAttributeCombinationGtin = SRC.ProductAttributeCombinationGtin,
        TGT.PartecipantFirstName = SRC.PartecipantFirstName,
        TGT.PartecipantLastName = SRC.PartecipantLastName,
        TGT.PartecipantEmail = SRC.PartecipantEmail,
        TGT.PartecipantFiscalCode = SRC.PartecipantFiscalCode,
        TGT.RootPartecipantEmail = SRC.RootPartecipantEmail

    WHEN NOT MATCHED AND SRC.IsDeleted = CAST(0 AS BIT)
      THEN INSERT VALUES (
        OrderItemId,
        PartecipantId,

        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
    
        OrderId,
        OrderTotal,
        OrderAuthorizationTransactionId,
        OrderPaidDate,
        OrderCreatedDate,
        OrderCustomerId,
        OrderStatusId,
        OrderNumber,
        OrderItemUnitPriceExclTax,
        OrderItemAttributeDescription,
        AttributeMappingId,
        AttributeValue,
        AttributeMappingId2,
        AttributeValue2,
        ProductId,
        ProductName,
        ProductShortDescription,
        ProductSku,
        ProductGtin,
        ProductSubdescription,
        ProductAttributeCombinationSku,
        ProductAttributeCombinationGtin,
        PartecipantFirstName,
        PartecipantLastName,
        PartecipantEmail,
        PartecipantFiscalCode,
        RootPartecipantEmail
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
        'Landing.MYSOLUTION_OrderItemPartecipant' AS full_olap_table_name,
        'OrderItemId = ' + CAST(COALESCE(inserted.OrderItemId, deleted.OrderItemId) AS NVARCHAR) + ', PartecipantId = ' + CAST(COALESCE(inserted.PartecipantId, deleted.PartecipantId) AS NVARCHAR) AS primary_key_description
    INTO audit.merge_log_details;

END;
GO

EXEC MYSOLUTION.usp_Merge_OrderItemPartecipant;
GO

/*
    SCHEMA_NAME > MYSOLUTION
    TABLE_NAME > StateProvince
*/

/**
 * @table Landing.MYSOLUTION_StateProvince
 * @description 

 * @depends MYSOLUTION.StateProvince

SELECT TOP 100 * FROM MYSOLUTION.StateProvince;
*/

IF OBJECT_ID('Landing.MYSOLUTION_StateProvinceView', 'V') IS NULL EXEC('CREATE VIEW Landing.MYSOLUTION_StateProvinceView AS SELECT 1 AS fld;');
GO

ALTER VIEW Landing.MYSOLUTION_StateProvinceView
AS
WITH TableData
AS (
    SELECT
        SP.Id,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            SP.Id,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            SP.CountryId,
            SP.Name,
            SP.Abbreviation,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
        SP.CountryId,
        SP.Name,
        SP.Abbreviation

    FROM MYSOLUTION.StateProvince SP
    INNER JOIN MYSOLUTION.Country C ON C.Id = SP.CountryId
        AND C.Published = CAST(1 AS BIT)
    WHERE SP.Published = CAST(1 AS BIT)
)
SELECT
    -- Chiavi
    TD.Id,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Attributi
    TD.CountryId,
    TD.Name,
    TD.Abbreviation

FROM TableData TD;
GO

--DROP TABLE IF EXISTS Landing.MYSOLUTION_StateProvince;
GO

IF OBJECT_ID(N'Landing.MYSOLUTION_StateProvince', N'U') IS NULL
BEGIN
    SELECT TOP 0 * INTO Landing.MYSOLUTION_StateProvince FROM Landing.MYSOLUTION_StateProvinceView;

    ALTER TABLE Landing.MYSOLUTION_StateProvince ADD CONSTRAINT PK_Landing_MYSOLUTION_StateProvince PRIMARY KEY CLUSTERED (UpdateDatetime, Id);

    --ALTER TABLE Landing.MYSOLUTION_StateProvince ALTER COLUMN  NVARCHAR(60) NOT NULL;

    CREATE UNIQUE NONCLUSTERED INDEX IX_MYSOLUTION_StateProvince_BusinessKey ON Landing.MYSOLUTION_StateProvince (Id);
END;
GO

IF OBJECT_ID('MYSOLUTION.usp_Merge_StateProvince', 'P') IS NULL EXEC('CREATE PROCEDURE MYSOLUTION.usp_Merge_StateProvince AS RETURN 0;');
GO

ALTER PROCEDURE MYSOLUTION.usp_Merge_StateProvince
AS
BEGIN
    SET NOCOUNT ON;

    MERGE INTO Landing.MYSOLUTION_StateProvince AS TGT
    USING Landing.MYSOLUTION_StateProvinceView (nolock) AS SRC
    ON SRC.Id = TGT.Id

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.CountryId = SRC.CountryId,
        TGT.Name = SRC.Name,
        TGT.Abbreviation = SRC.Abbreviation

    WHEN NOT MATCHED AND SRC.IsDeleted = CAST(0 AS BIT)
      THEN INSERT VALUES (
        Id,

        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
    
        CountryId,
        Name,
        Abbreviation
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
        'Landing.MYSOLUTION_StateProvince' AS full_olap_table_name,
        'Id = ' + CAST(COALESCE(inserted.Id, deleted.Id) AS NVARCHAR) AS primary_key_description
    INTO audit.merge_log_details;

END;
GO

EXEC MYSOLUTION.usp_Merge_StateProvince;
GO

/**
 * @table Landing.MYSOLUTION_Courses
 * @description 

 * @depends MYSOLUTION.Courses

SELECT TOP 100 * FROM MYSOLUTION.Courses;
*/

IF OBJECT_ID('Landing.MYSOLUTION_CoursesView', 'V') IS NULL EXEC('CREATE VIEW Landing.MYSOLUTION_CoursesView AS SELECT 1 AS fld;');
GO

ALTER VIEW Landing.MYSOLUTION_CoursesView
AS
WITH TableData
AS (
    SELECT
        C.OrderItemId,
        C.Partecipant_Id,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            C.OrderItemId,
            C.Partecipant_Id,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            C.PartecipantFirstName,
            C.PartecipantLastName,
            C.PartecipantEmail,
            C.PartecipantFiscalCode,
            C.RootPartecipantEmail,
            C.CustomerUserName,
            C.CourseName,
            C.CourseType,
            C.StartDate_text,
            C.StartDate,
            C.OrderNumber,
            C.OrderCreatedDate,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
        C.PartecipantFirstName,
        C.PartecipantLastName,
        C.PartecipantEmail,
        C.PartecipantFiscalCode,
        C.RootPartecipantEmail,
        C.CustomerUserName,
        C.CourseName,
        C.CourseType,
        C.StartDate_text,
        C.StartDate,
        C.OrderNumber,
        C.OrderCreatedDate

    FROM MYSOLUTION.Courses C
)
SELECT
    -- Chiavi
    TD.OrderItemId,
    TD.Partecipant_Id,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Attributi
    TD.PartecipantFirstName,
    TD.PartecipantLastName,
    TD.PartecipantEmail,
    TD.PartecipantFiscalCode,
    TD.RootPartecipantEmail,
    TD.CustomerUserName,
    TD.CourseName,
    TD.CourseType,
    TD.StartDate_text,
    TD.StartDate,
    TD.OrderNumber,
    TD.OrderCreatedDate

FROM TableData TD;
GO

--DROP TABLE IF EXISTS Landing.MYSOLUTION_Courses;
GO

IF OBJECT_ID(N'Landing.MYSOLUTION_Courses', N'U') IS NULL
BEGIN
    SELECT TOP 0 * INTO Landing.MYSOLUTION_Courses FROM Landing.MYSOLUTION_CoursesView;

    ALTER TABLE Landing.MYSOLUTION_Courses ADD CONSTRAINT PK_Landing_MYSOLUTION_Courses PRIMARY KEY CLUSTERED (UpdateDatetime, OrderItemId, Partecipant_Id);

    --ALTER TABLE Landing.MYSOLUTION_Courses ALTER COLUMN  NVARCHAR(60) NOT NULL;

    CREATE UNIQUE NONCLUSTERED INDEX IX_MYSOLUTION_Courses_BusinessKey ON Landing.MYSOLUTION_Courses (OrderItemId, Partecipant_Id);
END;
GO

IF OBJECT_ID('MYSOLUTION.usp_Merge_Courses', 'P') IS NULL EXEC('CREATE PROCEDURE MYSOLUTION.usp_Merge_Courses AS RETURN 0;');
GO

ALTER PROCEDURE MYSOLUTION.usp_Merge_Courses
AS
BEGIN
    SET NOCOUNT ON;

    MERGE INTO Landing.MYSOLUTION_Courses AS TGT
    USING Landing.MYSOLUTION_CoursesView (NOLOCK) AS SRC
    ON SRC.OrderItemId = TGT.OrderItemId AND SRC.Partecipant_Id = TGT.Partecipant_Id

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.OrderItemId = SRC.OrderItemId,
        TGT.Partecipant_Id = SRC.Partecipant_Id,
        TGT.PartecipantFirstName = SRC.PartecipantFirstName,
        TGT.PartecipantLastName = SRC.PartecipantLastName,
        TGT.PartecipantEmail = SRC.PartecipantEmail,
        TGT.PartecipantFiscalCode = SRC.PartecipantFiscalCode,
        TGT.RootPartecipantEmail = SRC.RootPartecipantEmail,
        TGT.CustomerUserName = SRC.CustomerUserName,
        TGT.CourseName = SRC.CourseName,
        TGT.CourseType = SRC.CourseType,
        TGT.StartDate_text = SRC.StartDate_text,
        TGT.StartDate = SRC.StartDate,
        TGT.OrderNumber = SRC.OrderNumber,
        TGT.OrderCreatedDate = SRC.OrderCreatedDate

    WHEN NOT MATCHED AND SRC.IsDeleted = CAST(0 AS BIT)
      THEN INSERT VALUES (
        OrderItemId,
        Partecipant_Id,

        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
    
        PartecipantFirstName,
        PartecipantLastName,
        PartecipantEmail,
        PartecipantFiscalCode,
        RootPartecipantEmail,
        CustomerUserName,
        CourseName,
        CourseType,
        StartDate_text,
        StartDate,
        OrderNumber,
        OrderCreatedDate
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
        'Landing.MYSOLUTION_Courses' AS full_olap_table_name,
        'OrderItemId = ' + CAST(COALESCE(inserted.OrderItemId, deleted.OrderItemId) AS NVARCHAR)
            + ', Partecipant_Id = ' + CAST(COALESCE(inserted.Partecipant_Id, deleted.Partecipant_Id) AS NVARCHAR) AS primary_key_description
    INTO audit.merge_log_details;

END;
GO

EXEC MYSOLUTION.usp_Merge_Courses;
GO

/**
 * @table Landing.MYSOLUTION_CoursesData
 * @description 

 * @depends MYSOLUTIONPRODUZIONE2.Nop_MySolution.dbo.VW_MySolution_Courses

SELECT TOP 100 * FROM MYSOLUTIONPRODUZIONE2.Nop_MySolution.dbo.VW_MySolution_Courses;
*/

IF OBJECT_ID('Landing.MYSOLUTION_CoursesDataView', 'V') IS NULL EXEC('CREATE VIEW Landing.MYSOLUTION_CoursesDataView AS SELECT 1 AS fld;');
GO

ALTER VIEW Landing.MYSOLUTION_CoursesDataView
AS
WITH TableData
AS (
    SELECT
        C.OrderItemId,
        C.Partecipant_Id,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            C.OrderItemId,
            C.Partecipant_Id,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            C.PartecipantFirstName,
            C.PartecipantLastName,
            C.PartecipantEmail,
            C.PartecipantFiscalCode,
            C.RootPartecipantEmail,
            C.CustomerUserName,
            C.CourseName,
            C.CourseCode,
            C.AttCourseCode,
            C.WebinarCode,
            C.AttWebinarCode,
            C.CourseType,
            C.StartDate_text,
            C.StartDate,
            C.HasMoreDates,
            C.OrderDescription,
            C.ItemNetUnitPrice,
            C.OrderTotalPrice,
            C.OrderNumber,
            C.OrderStatus,
            C.OrderCreatedDate,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
        C.PartecipantFirstName,
        C.PartecipantLastName,
        C.PartecipantEmail,
        C.PartecipantFiscalCode,
        C.RootPartecipantEmail,
        C.CustomerUserName,
        C.CourseName,
        C.CourseCode,
        C.AttCourseCode,
        C.WebinarCode,
        C.AttWebinarCode,
        C.CourseType,
        C.StartDate_text,
        C.StartDate,
        C.HasMoreDates,
        C.OrderDescription,
        C.ItemNetUnitPrice,
        C.OrderTotalPrice,
        C.OrderNumber,
        C.OrderStatus,
        CAST(C.OrderCreatedDate AS DATE) AS OrderCreatedDate

    FROM MYSOLUTIONPRODUZIONE2.Nop_MySolution.dbo.VW_MySolution_Courses C
)
SELECT
    -- Chiavi
    TD.OrderItemId,
    TD.Partecipant_Id,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Attributi
    TD.PartecipantFirstName,
    TD.PartecipantLastName,
    TD.PartecipantEmail,
    TD.PartecipantFiscalCode,
    TD.RootPartecipantEmail,
    TD.CustomerUserName,
    TD.CourseName,
    TD.CourseCode,
    TD.AttCourseCode,
    TD.WebinarCode,
    TD.AttWebinarCode,
    TD.CourseType,
    TD.StartDate_text,
    TD.StartDate,
    TD.HasMoreDates,
    TD.OrderDescription,
    TD.ItemNetUnitPrice,
    TD.OrderTotalPrice,
    TD.OrderNumber,
    TD.OrderStatus,
    TD.OrderCreatedDate

FROM TableData TD;
GO

--DROP TABLE IF EXISTS Landing.MYSOLUTION_CoursesData;
GO

IF OBJECT_ID(N'Landing.MYSOLUTION_CoursesData', N'U') IS NULL
BEGIN
    SELECT TOP 0 * INTO Landing.MYSOLUTION_CoursesData FROM Landing.MYSOLUTION_CoursesDataView;

    ALTER TABLE Landing.MYSOLUTION_CoursesData ADD CONSTRAINT PK_Landing_MYSOLUTION_CoursesData PRIMARY KEY CLUSTERED (UpdateDatetime, OrderItemId, Partecipant_Id);

    --ALTER TABLE Landing.MYSOLUTION_CoursesData ALTER COLUMN  NVARCHAR(60) NOT NULL;

    CREATE UNIQUE NONCLUSTERED INDEX IX_MYSOLUTION_CoursesData_BusinessKey ON Landing.MYSOLUTION_CoursesData (OrderItemId, Partecipant_Id);
END;
GO

IF OBJECT_ID('MYSOLUTION.usp_Merge_CoursesData', 'P') IS NULL EXEC('CREATE PROCEDURE MYSOLUTION.usp_Merge_CoursesData AS RETURN 0;');
GO

ALTER PROCEDURE MYSOLUTION.usp_Merge_CoursesData
AS
BEGIN
    SET NOCOUNT ON;

    MERGE INTO Landing.MYSOLUTION_CoursesData AS TGT
    USING Landing.MYSOLUTION_CoursesDataView (NOLOCK) AS SRC
    ON SRC.OrderItemId = TGT.OrderItemId AND SRC.Partecipant_Id = TGT.Partecipant_Id

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.PartecipantFirstName = SRC.PartecipantFirstName,
        TGT.PartecipantLastName = SRC.PartecipantLastName,
        TGT.PartecipantEmail = SRC.PartecipantEmail,
        TGT.PartecipantFiscalCode = SRC.PartecipantFiscalCode,
        TGT.RootPartecipantEmail = SRC.RootPartecipantEmail,
        TGT.CustomerUserName = SRC.CustomerUserName,
        TGT.CourseName = SRC.CourseName,
        TGT.CourseCode = SRC.CourseCode,
        TGT.AttCourseCode = SRC.AttCourseCode,
        TGT.WebinarCode = SRC.WebinarCode,
        TGT.AttWebinarCode = SRC.AttWebinarCode,
        TGT.CourseType = SRC.CourseType,
        TGT.StartDate_text = SRC.StartDate_text,
        TGT.StartDate = SRC.StartDate,
        TGT.HasMoreDates = SRC.HasMoreDates,
        TGT.OrderDescription = SRC.OrderDescription,
        TGT.ItemNetUnitPrice = SRC.ItemNetUnitPrice,
        TGT.OrderTotalPrice = SRC.OrderTotalPrice,
        TGT.OrderNumber = SRC.OrderNumber,
        TGT.OrderStatus = SRC.OrderStatus,
        TGT.OrderCreatedDate = SRC.OrderCreatedDate

    WHEN NOT MATCHED AND SRC.IsDeleted = CAST(0 AS BIT)
      THEN INSERT VALUES (
        OrderItemId,
        Partecipant_Id,

        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
    
        PartecipantFirstName,
        PartecipantLastName,
        PartecipantEmail,
        PartecipantFiscalCode,
        RootPartecipantEmail,
        CustomerUserName,
        CourseName,
        CourseCode,
        AttCourseCode,
        WebinarCode,
        AttWebinarCode,
        CourseType,
        StartDate_text,
        StartDate,
        HasMoreDates,
        OrderDescription,
        ItemNetUnitPrice,
        OrderTotalPrice,
        OrderNumber,
        OrderStatus,
        OrderCreatedDate
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
        'Landing.MYSOLUTION_CoursesData' AS full_olap_table_name,
        'OrderItemId = ' + CAST(COALESCE(inserted.OrderItemId, deleted.OrderItemId) AS NVARCHAR)
            + ', Partecipant_Id = ' + CAST(COALESCE(inserted.Partecipant_Id, deleted.Partecipant_Id) AS NVARCHAR) AS primary_key_description
    INTO audit.merge_log_details;

END;
GO

EXEC MYSOLUTION.usp_Merge_CoursesData;
GO

/**
 * @table Landing.MYSOLUTION_Users
 * @description 

 * @depends MYSOLUTION.Users

SELECT TOP 100 * FROM MYSOLUTION.Users;
*/

IF OBJECT_ID('Landing.MYSOLUTION_UsersView', 'V') IS NULL EXEC('CREATE VIEW Landing.MYSOLUTION_UsersView AS SELECT 1 AS fld;');
GO

ALTER VIEW Landing.MYSOLUTION_UsersView
AS
WITH TableData
AS (
    SELECT
        U.ID,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            U.ID,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            U.EMAIL,
            U.RagioneSociale,
            U.Nome,
            U.Cognome,
            U.Citta,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
        U.EMAIL AS Email,
        U.RagioneSociale,
        U.Nome,
        U.Cognome,
        U.Citta

    FROM MYSOLUTION.Users U
)
SELECT
    -- Chiavi
    TD.ID,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Attributi
    TD.Email,
    TD.RagioneSociale,
    TD.Nome,
    TD.Cognome,
    TD.Citta

FROM TableData TD;
GO

--DROP TABLE IF EXISTS Landing.MYSOLUTION_Users;
GO

IF OBJECT_ID(N'Landing.MYSOLUTION_Users', N'U') IS NULL
BEGIN
    SELECT TOP 0 * INTO Landing.MYSOLUTION_Users FROM Landing.MYSOLUTION_UsersView;

    ALTER TABLE Landing.MYSOLUTION_Users ADD CONSTRAINT PK_Landing_MYSOLUTION_Users PRIMARY KEY CLUSTERED (UpdateDatetime, ID);

    --ALTER TABLE Landing.MYSOLUTION_Users ALTER COLUMN  NVARCHAR(60) NOT NULL;

    CREATE UNIQUE NONCLUSTERED INDEX IX_MYSOLUTION_Users_BusinessKey ON Landing.MYSOLUTION_Users (ID);
END;
GO

IF OBJECT_ID('MYSOLUTION.usp_Merge_Users', 'P') IS NULL EXEC('CREATE PROCEDURE MYSOLUTION.usp_Merge_Users AS RETURN 0;');
GO

ALTER PROCEDURE MYSOLUTION.usp_Merge_Users
AS
BEGIN
    SET NOCOUNT ON;

    MERGE INTO Landing.MYSOLUTION_Users AS TGT
    USING Landing.MYSOLUTION_UsersView (nolock) AS SRC
    ON SRC.ID = TGT.ID

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.Email = SRC.Email,
        TGT.RagioneSociale = SRC.RagioneSociale,
        TGT.Nome = SRC.Nome,
        TGT.Cognome = SRC.Cognome,
        TGT.Citta = SRC.Citta

    WHEN NOT MATCHED AND SRC.IsDeleted = CAST(0 AS BIT)
      THEN INSERT VALUES (
        ID,

        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
    
        Email,
        RagioneSociale,
        Nome,
        Cognome,
        Citta
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
        'Landing.MYSOLUTION_Users' AS full_olap_table_name,
        'ID = ' + CAST(COALESCE(inserted.ID, deleted.ID) AS NVARCHAR) AS primary_key_description
    INTO audit.merge_log_details;

END;
GO

EXEC MYSOLUTION.usp_Merge_Users;
GO

/**
 * @table Landing.MYSOLUTION_NopCustomer
 * @description 

 * @depends MYSOLUTIONPRODUZIONE2.Nop_MySolution.dbo.Customer

SELECT TOP 100 * FROM MYSOLUTIONPRODUZIONE2.Nop_MySolution.dbo.Customer;
*/

IF OBJECT_ID('Landing.MYSOLUTION_NopCustomerView', 'V') IS NULL EXEC('CREATE VIEW Landing.MYSOLUTION_NopCustomerView AS SELECT 1 AS fld;');
GO

ALTER VIEW Landing.MYSOLUTION_NopCustomerView
AS
WITH TableData
AS (
    SELECT
        C.Id,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            C.Id,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            C.Username,
            C.Email,
            C.Active,
            C.Deleted,
            C.BillingAddress_Id,
            C.ShippingAddress_Id,
            C.IdCometa,
            C.DateExpiration,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
        C.Username,
        C.Email,
        C.Active,
        C.Deleted,
        C.BillingAddress_Id,
        C.ShippingAddress_Id,
        C.IdCometa,
        C.DateExpiration

    FROM MySOLUTION.Customer C
)
SELECT
    -- Chiavi
    TD.Id,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Attributi
    TD.Username,
    TD.Email,
    TD.Active,
    TD.Deleted,
    TD.BillingAddress_Id,
    TD.ShippingAddress_Id,
    TD.IdCometa,
    TD.DateExpiration

FROM TableData TD;
GO

--DROP TABLE IF EXISTS Landing.MYSOLUTION_NopCustomer;
GO

IF OBJECT_ID(N'Landing.MYSOLUTION_NopCustomer', N'U') IS NULL
BEGIN
    SELECT TOP 0 * INTO Landing.MYSOLUTION_NopCustomer FROM Landing.MYSOLUTION_NopCustomerView;

    ALTER TABLE Landing.MYSOLUTION_NopCustomer ADD CONSTRAINT PK_Landing_MYSOLUTION_NopCustomer PRIMARY KEY CLUSTERED (UpdateDatetime, Id);

    --ALTER TABLE Landing.MYSOLUTION_NopCustomer ALTER COLUMN  NVARCHAR(60) NOT NULL;

    CREATE UNIQUE NONCLUSTERED INDEX IX_MYSOLUTION_NopCustomer_BusinessKey ON Landing.MYSOLUTION_NopCustomer (Id);
END;
GO

IF OBJECT_ID('MYSOLUTION.usp_Merge_NopCustomer', 'P') IS NULL EXEC('CREATE PROCEDURE MYSOLUTION.usp_Merge_NopCustomer AS RETURN 0;');
GO

ALTER PROCEDURE MYSOLUTION.usp_Merge_NopCustomer
AS
BEGIN
    SET NOCOUNT ON;

    MERGE INTO Landing.MYSOLUTION_NopCustomer AS TGT
    USING Landing.MYSOLUTION_NopCustomerView (NOLOCK) AS SRC
    ON SRC.Id = TGT.Id

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.Username = SRC.Username,
        TGT.Email = SRC.Email,
        TGT.Active = SRC.Active,
        TGT.Deleted = SRC.Deleted,
        TGT.BillingAddress_Id = SRC.BillingAddress_Id,
        TGT.ShippingAddress_Id = SRC.ShippingAddress_Id,
        TGT.IdCometa = SRC.IdCometa,
        TGT.DateExpiration = SRC.DateExpiration

    WHEN NOT MATCHED AND SRC.IsDeleted = CAST(0 AS BIT)
      THEN INSERT VALUES (
        Id,

        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
    
        Username,
        Email,
        Active,
        Deleted,
        BillingAddress_Id,
        ShippingAddress_Id,
        IdCometa,
        DateExpiration
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
        'Landing.MYSOLUTION_NopCustomer' AS full_olap_table_name,
        'Id = ' + CAST(COALESCE(inserted.Id, deleted.Id) AS NVARCHAR) AS primary_key_description
    INTO audit.merge_log_details;

END;
GO

EXEC MYSOLUTION.usp_Merge_NopCustomer;
GO

/**
 * @table Landing.MYSOLUTION_CustomerPartecipants
 * @description 

 * @depends MYSOLUTIONPRODUZIONE2.Nop_MySolution.dbo.Customer

SELECT TOP 100 * FROM MYSOLUTIONPRODUZIONE2.Nop_MySolution.dbo.Customer;
*/

IF OBJECT_ID('Landing.MYSOLUTION_CustomerPartecipantsView', 'V') IS NULL EXEC('CREATE VIEW Landing.MYSOLUTION_CustomerPartecipantsView AS SELECT 1 AS fld;');
GO

ALTER VIEW Landing.MYSOLUTION_CustomerPartecipantsView
AS
WITH TableData
AS (
    SELECT
        CP.Customer_Id,
        CP.Partecipant_Id,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            CP.Customer_Id,
            CP.Partecipant_Id,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            CP.Customer_Id,
            CP.Partecipant_Id,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime

    FROM MYSOLUTION.CustomerPartecipants CP
)
SELECT
    -- Chiavi
    TD.Customer_Id,
    TD.Partecipant_Id,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted

FROM TableData TD;
GO

--DROP TABLE IF EXISTS Landing.MYSOLUTION_CustomerPartecipants;
GO

IF OBJECT_ID(N'Landing.MYSOLUTION_CustomerPartecipants', N'U') IS NULL
BEGIN
    SELECT TOP 0 * INTO Landing.MYSOLUTION_CustomerPartecipants FROM Landing.MYSOLUTION_CustomerPartecipantsView;

    ALTER TABLE Landing.MYSOLUTION_CustomerPartecipants ADD CONSTRAINT PK_Landing_MYSOLUTION_CustomerPartecipants PRIMARY KEY CLUSTERED (UpdateDatetime, Customer_Id, Partecipant_Id);

    --ALTER TABLE Landing.MYSOLUTION_CustomerPartecipants ALTER COLUMN  NVARCHAR(60) NOT NULL;

    CREATE UNIQUE NONCLUSTERED INDEX IX_MYSOLUTION_CustomerPartecipants_BusinessKey ON Landing.MYSOLUTION_CustomerPartecipants (Customer_Id, Partecipant_Id);
END;
GO

IF OBJECT_ID('MYSOLUTION.usp_Merge_CustomerPartecipants', 'P') IS NULL EXEC('CREATE PROCEDURE MYSOLUTION.usp_Merge_CustomerPartecipants AS RETURN 0;');
GO

ALTER PROCEDURE MYSOLUTION.usp_Merge_CustomerPartecipants
AS
BEGIN
    SET NOCOUNT ON;

    MERGE INTO Landing.MYSOLUTION_CustomerPartecipants AS TGT
    USING Landing.MYSOLUTION_CustomerPartecipantsView (NOLOCK) AS SRC
    ON SRC.Customer_Id = TGT.Customer_Id
        AND SRC.Partecipant_Id = TGT.Partecipant_Id

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime

    WHEN NOT MATCHED AND SRC.IsDeleted = CAST(0 AS BIT)
      THEN INSERT VALUES (
        Customer_Id,
        Partecipant_Id,

        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted
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
        'Landing.MYSOLUTION_CustomerPartecipants' AS full_olap_table_name,
        'Customer_Id = ' + CAST(COALESCE(inserted.Customer_Id, deleted.Customer_Id) AS NVARCHAR)
            + ', Partecipant_Id = ' + CAST(COALESCE(inserted.Partecipant_Id, deleted.Partecipant_Id) AS NVARCHAR)
        AS primary_key_description
    INTO audit.merge_log_details;

END;
GO

EXEC MYSOLUTION.usp_Merge_CustomerPartecipants;
GO

/**
 * @table Landing.MYSOLUTION_Partecipant
 * @description 

 * @depends MYSOLUTION.Partecipant

SELECT TOP 100 * FROM MYSOLUTION.Partecipant;
*/

IF OBJECT_ID('Landing.MYSOLUTION_PartecipantView', 'V') IS NULL EXEC('CREATE VIEW Landing.MYSOLUTION_PartecipantView AS SELECT 1 AS fld;');
GO

ALTER VIEW Landing.MYSOLUTION_PartecipantView
AS
WITH TableData
AS (
    SELECT
        P.Id,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            P.Id,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            P.FirstName,
            P.LastName,
            P.Email,
            P.PhoneNumber,
            P.Ssn,
            P.CreatedOnUtc,
            P.IdProfession,
            P.IdProfessionDetail,
            P.MobilePhone,
            P.OriginalPartecipantId,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
        P.FirstName,
        P.LastName,
        P.Email,
        P.PhoneNumber,
        P.Ssn,
        P.CreatedOnUtc,
        P.IdProfession,
        P.IdProfessionDetail,
        P.MobilePhone,
        P.OriginalPartecipantId

    FROM MySOLUTION.Partecipant P
)
SELECT
    -- Chiavi
    TD.Id,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Attributi
    TD.FirstName,
    TD.LastName,
    TD.Email,
    TD.PhoneNumber,
    TD.Ssn,
    TD.CreatedOnUtc,
    TD.IdProfession,
    TD.IdProfessionDetail,
    TD.MobilePhone,
    TD.OriginalPartecipantId

FROM TableData TD;
GO

--DROP TABLE IF EXISTS Landing.MYSOLUTION_Partecipant;
GO

IF OBJECT_ID(N'Landing.MYSOLUTION_Partecipant', N'U') IS NULL
BEGIN
    SELECT TOP 0 * INTO Landing.MYSOLUTION_Partecipant FROM Landing.MYSOLUTION_PartecipantView;

    ALTER TABLE Landing.MYSOLUTION_Partecipant ADD CONSTRAINT PK_Landing_MYSOLUTION_Partecipant PRIMARY KEY CLUSTERED (UpdateDatetime, Id);

    --ALTER TABLE Landing.MYSOLUTION_Partecipant ALTER COLUMN  NVARCHAR(60) NOT NULL;

    CREATE UNIQUE NONCLUSTERED INDEX IX_MYSOLUTION_Partecipant_BusinessKey ON Landing.MYSOLUTION_Partecipant (Id);
END;
GO

IF OBJECT_ID('MYSOLUTION.usp_Merge_Partecipant', 'P') IS NULL EXEC('CREATE PROCEDURE MYSOLUTION.usp_Merge_Partecipant AS RETURN 0;');
GO

ALTER PROCEDURE MYSOLUTION.usp_Merge_Partecipant
AS
BEGIN
    SET NOCOUNT ON;

    MERGE INTO Landing.MYSOLUTION_Partecipant AS TGT
    USING Landing.MYSOLUTION_PartecipantView (NOLOCK) AS SRC
    ON SRC.Id = TGT.Id

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.FirstName = SRC.FirstName,
        TGT.LastName = SRC.LastName,
        TGT.Email = SRC.Email,
        TGT.PhoneNumber = SRC.PhoneNumber,
        TGT.Ssn = SRC.Ssn,
        TGT.CreatedOnUtc = SRC.CreatedOnUtc,
        TGT.IdProfession = SRC.IdProfession,
        TGT.IdProfessionDetail = SRC.IdProfessionDetail,
        TGT.MobilePhone = SRC.MobilePhone,
        TGT.OriginalPartecipantId = SRC.OriginalPartecipantId

    WHEN NOT MATCHED AND SRC.IsDeleted = CAST(0 AS BIT)
      THEN INSERT VALUES (
        Id,

        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
    
        FirstName,
        LastName,
        Email,
        PhoneNumber,
        Ssn,
        CreatedOnUtc,
        IdProfession,
        IdProfessionDetail,
        MobilePhone,
        OriginalPartecipantId
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
        'Landing.MYSOLUTION_Partecipant' AS full_olap_table_name,
        'Id = ' + CAST(COALESCE(inserted.Id, deleted.Id) AS NVARCHAR) AS primary_key_description
    INTO audit.merge_log_details;

END;
GO

EXEC MYSOLUTION.usp_Merge_Partecipant;
GO
