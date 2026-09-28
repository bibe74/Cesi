
CREATE   PROCEDURE Staging.usp_Reload_CometaVendor
AS
BEGIN

    SET NOCOUNT ON;

    BEGIN TRANSACTION 

    TRUNCATE TABLE Staging.CometaVendor;

    INSERT INTO Staging.CometaVendor SELECT * FROM Staging.CometaVendorView;

    COMMIT TRANSACTION 

END;

GO

