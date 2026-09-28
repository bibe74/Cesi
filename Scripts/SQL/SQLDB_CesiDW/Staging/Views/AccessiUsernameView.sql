
/**
 * @table Staging.AccessiUsername
 * @description
*/

CREATE   VIEW Staging.AccessiUsernameView
AS
SELECT DISTINCT
    LFR.Username AS UsernameAccessi,
    LTRIM(RTRIM(LOWER(LFR.Username))) AS Username

FROM Landing.MYSOLUTION_LogsForReport LFR
WHERE COALESCE(LTRIM(RTRIM(LOWER(LFR.Username))), N'') LIKE N'%@%';

GO

