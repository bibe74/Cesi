
CREATE   PROCEDURE Staging.usp_Reload_MySolutionCustomer
AS
BEGIN

    SET NOCOUNT ON;

    BEGIN TRANSACTION 

    TRUNCATE TABLE Staging.MySolutionCustomer;

    INSERT INTO Staging.MySolutionCustomer SELECT * FROM Staging.MySolutionCustomerView;

    COMMIT TRANSACTION 

END;

GO

