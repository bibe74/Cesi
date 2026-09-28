
CREATE   PROCEDURE Dim.usp_ChiusuraOrdinazione (
    @DataOraEsecuzione DATETIME = NULL
)
AS
BEGIN

    SET NOCOUNT ON;

    IF @DataOraEsecuzione IS NULL SET @DataOraEsecuzione = CURRENT_TIMESTAMP;

    UPDATE Dim.Data SET IsOrdinazioneMensileChiusa = CAST(0 AS BIT), IsOrdinazioneChiusa = CAST(0 AS BIT);

    UPDATE Dim.Data
    SET IsOrdinazioneChiusa = CAST(1 AS BIT)
    WHERE Anno < DATEPART(YEAR, DATEADD(DAY, -DATEPART(DAY, @DataOraEsecuzione), @DataOraEsecuzione))
	    OR (
		    Anno = DATEPART(YEAR, DATEADD(DAY, -DATEPART(DAY, @DataOraEsecuzione), @DataOraEsecuzione))
		    AND Mese <= DATEPART(MONTH, DATEADD(DAY, -DATEPART(DAY, @DataOraEsecuzione), @DataOraEsecuzione))
	    );

    UPDATE Dim.Data
    SET IsOrdinazioneMensileChiusa = CAST(1 AS BIT)
    WHERE Anno <= DATEPART(YEAR, DATEADD(DAY, -DATEPART(DAY, @DataOraEsecuzione), @DataOraEsecuzione))
	    AND Mese <= DATEPART(MONTH, DATEADD(DAY, -DATEPART(DAY, @DataOraEsecuzione), @DataOraEsecuzione));

END;

GO

