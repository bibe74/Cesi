
CREATE   PROCEDURE Staging.usp_Reload_AccessiCustomer
AS
BEGIN

    SET NOCOUNT ON;

    BEGIN TRANSACTION 

    TRUNCATE TABLE Staging.AccessiCustomer;

    INSERT INTO Staging.AccessiCustomer SELECT * FROM Staging.AccessiCustomerView;

    COMMIT TRANSACTION 

END;

GO

