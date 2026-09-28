
CREATE   VIEW Staging.DomandeMIAView
AS
WITH Clienti
AS (
    SELECT
        Id,
        Email

    FROM Landing.GPT_OpenAICliente C
    WHERE C.IsDeleted = CAST(0 AS BIT)
        AND (
            C.Email NOT LIKE '%cesimultimedia%'
            AND C.Email NOT LIKE '%partnerup%'
            AND C.Email NOT LIKE '%vincenti%'
            AND C.Email NOT LIKE '%giuggioli%'
        )
),
DomandeMIA
AS (
    SELECT
        M.Id,
        LTRIM (
            RTRIM (
                SUBSTRING (
                    REPLACE (
                        REPLACE (
                            REPLACE (
                                CASE
                                    WHEN CHARINDEX ('§§END-EXTRA-PROMPT-CESI§§', M.Message) > 0 THEN
                                        -- Trova l'ultima posizione invertendo la stringa
                                        SUBSTRING (
                                            M.Message,
                                            LEN (M.Message) - CHARINDEX ('§§ISEC-TPMORP-ARTXE-DNE§§', REVERSE (M.Message))
                                            + 2,
                                            LEN (M.Message)
                                        )
                                    ELSE
                                        M.Message
                                END,
                                CHAR (13),
                                ' '
                            ),
                            CHAR (10),
                            ' '
                        ),
                        CHAR (9),
                        ' '
                    ),
                    0,
                    4000
                )
            )
        ) AS Testo,
        CONVERT(BIT, M.IsQuestion) AS IsDomanda,
        M.CreatedOn AS DataOraCreazione,
        CONVERT(DATE, M.CreatedOn) AS DataCreazione,
        T.Area,
        C.Email

    FROM Landing.GPT_OpenAIMessage M
    INNER JOIN Landing.GPT_OpenAIThread T ON T.Id = M.OpenAIThreadId
        AND T.IsDeleted = CAST(0 AS BIT)
    INNER JOIN Clienti C ON C.Id = T.ClienteId
    WHERE M.IsDeleted = CAST(0 AS BIT)
        AND M.CreatedOn >= CONVERT(DATETIME, DATEADD(DAY, -30, CONVERT(DATE, CURRENT_TIMESTAMP)))
),
TableData
AS (
    SELECT
        D.Id,

        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            D.Id,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            D.Testo,
            D.IsDomanda,
            --D.DataOraCreazione,
            --D.DataCreazione,
            DC.PKData,
            D.Area,
            D.Email,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,

        D.Testo,
        D.IsDomanda,
        --D.DataOraCreazione,
        --D.DataCreazione,
        DC.PKData AS PKDataCreazione,
        D.Area,
        D.Email

    FROM DomandeMIA D
    INNER JOIN Dim.Data DC ON DC.PKData = D.DataCreazione
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
    TD.Testo,
    TD.IsDomanda,
    TD.PKDataCreazione,
    TD.Area,
    TD.Email

FROM TableData TD;

GO

