CREATE   VIEW Staging.ClientiNOPInCometaView
AS
WITH ClientiNOPInCometaDettaglio
AS (
    SELECT DISTINCT
        NOP.PKCliente AS PKClienteNOP,
        NOP.Email,
        COM.PKCliente AS PKClienteCometa

    FROM Dim.Cliente NOP
    INNER JOIN Dim.Cliente COM ON COM.Email = NOP.Email
        AND COM.ProvenienzaAnagrafica = N'COMETA'
        AND COM.IsDeleted = CAST(0 AS BIT)
    WHERE NOP.ProvenienzaAnagrafica = N'NOP'
        AND NOP.IsDeleted = CAST(0 AS BIT)
),
ClientiNOPInCometa
AS (
    SELECT
        CNICD.PKClienteNOP,
        CNICD.Email,
        CNICD.PKClienteCometa,
        ROW_NUMBER() OVER (PARTITION BY CNICD.PKClienteNOP ORDER BY CNICD.PKClienteCometa DESC) AS rn

    FROM ClientiNOPInCometaDettaglio CNICD
)
SELECT
    CNIC.PKClienteNOP,
    CNIC.Email,
    CNIC.PKClienteCometa

FROM ClientiNOPInCometa CNIC
WHERE CNIC.rn = 1;

GO

