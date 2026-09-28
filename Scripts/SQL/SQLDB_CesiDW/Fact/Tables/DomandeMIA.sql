CREATE TABLE [Fact].[DomandeMIA] (
    [PKDomandeMIA]           INT             CONSTRAINT [DFT_Fact_DomandeMIA_PKDomandeMIA] DEFAULT (NEXT VALUE FOR [dbo].[seq_Fact_DomandeMIA]) NOT NULL,
    [Id]                     INT             NOT NULL,
    [HistoricalHashKey]      VARBINARY (20)  NULL,
    [ChangeHashKey]          VARBINARY (20)  NULL,
    [HistoricalHashKeyASCII] VARCHAR (34)    NULL,
    [ChangeHashKeyASCII]     VARCHAR (34)    NULL,
    [InsertDatetime]         DATETIME        NOT NULL,
    [UpdateDatetime]         DATETIME        NOT NULL,
    [IsDeleted]              BIT             NOT NULL,
    [PKDataCreazione]        DATE            NOT NULL,
    [Testo]                  NVARCHAR (4000) NULL,
    [IsDomanda]              BIT             NOT NULL,
    [Area]                   NVARCHAR (80)   NULL,
    [Email]                  NVARCHAR (60)   NULL,
    CONSTRAINT [PK_Fact_DomandeMIA] PRIMARY KEY CLUSTERED ([PKDomandeMIA] ASC),
    CONSTRAINT [FK_Fact_DomandeMIA_PKDataCreazione] FOREIGN KEY ([PKDataCreazione]) REFERENCES [Dim].[Data] ([PKData])
);


GO

