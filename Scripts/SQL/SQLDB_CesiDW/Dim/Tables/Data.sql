CREATE TABLE [Dim].[Data] (
    [PKData]                     DATE          NOT NULL,
    [Data_IT]                    VARCHAR (10)  NOT NULL,
    [Anno]                       INT           NOT NULL,
    [Mese]                       INT           NOT NULL,
    [Mese_IT]                    VARCHAR (10)  NOT NULL,
    [AnnoMese]                   INT           NOT NULL,
    [AnnoMese_IT]                VARCHAR (20)  NOT NULL,
    [Settimana]                  INT           NOT NULL,
    [AnnoSettimana]              INT           NOT NULL,
    [AnnoSettimana_IT]           VARCHAR (20)  NOT NULL,
    [SettimanaDescrizione]       NVARCHAR (24) NOT NULL,
    [IsOrdinazioneChiusa]        BIT           CONSTRAINT [DFT_Dim_Data_IsOrdinazioneChiusa] DEFAULT ((0)) NOT NULL,
    [IsOrdinazioneMensileChiusa] BIT           CONSTRAINT [DFT_Dim_Data_IsOrdinazioneMensileChiusa] DEFAULT ((0)) NOT NULL,
    [GiornoSettimana]            INT           NULL,
    CONSTRAINT [PK_Dim_Data] PRIMARY KEY CLUSTERED ([PKData] ASC)
);


GO

