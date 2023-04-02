IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[RateLimterSettings]') AND TYPE IN (N'U'))
BEGIN
    DROP TABLE [dbo].RateLimterSettings;
END

GO

CREATE TABLE RateLimterSettings (
    [RateLimterSettingsId]       INT PRIMARY KEY,
    [IpRateLimiting]             VARCHAR(MAX) NULL,
    [IpRateLimitingPolicy]       VARCHAR(MAX) NULL,
    [ClientRateLimiting]         VARCHAR(MAX) NULL,
    [ClientRateLimitingPolicy]   VARCHAR(MAX) NULL
);

GO

IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[RateLimterCache]') AND TYPE IN (N'U'))
BEGIN
    DROP TABLE [dbo].[RateLimterCache];
END

GO

CREATE TABLE [RateLimterCache] (
    [CounterId] VARCHAR(256) PRIMARY KEY,
    [Key]       VARCHAR(MAX) NOT NULL,
    [Count]     FLOAT        NOT NULL,
    [Expiry]    DATETIME2(7) NOT NULL
);

GO

CREATE INDEX [IX_myCacheTable_expiry] ON [RateLimterCache] ([expiry]);

GO

CREATE PROCEDURE dbo.IncrementCounter
    @counterId  VARCHAR(256),
    @key        VARCHAR(MAX) NOT NULL,
    @delta      BIGINT,
    @timeout    BIGINT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @count FLOAT;
    DECLARE @ttl INT;

    BEGIN TRY
        BEGIN TRANSACTION;

        IF EXISTS (SELECT [CounterId] FROM [RateLimterCache] WHERE [CounterId] = @counterId)
        BEGIN
            --i think this should be 
            --SET @count = @delta - CAST((SELECT [Count] FROM [RateLimterCache] WHERE [CounterId] = @counterId) AS FLOAT);
            --but i'm not sure
            SET @count = CAST((SELECT [Count] FROM [RateLimterCache] WHERE [CounterId] = @counterId) AS FLOAT) + @delta; 
            IF @count < 0
                SET @count = 0;

            SET @ttl = DATEDIFF(second, GETUTCDATE(), (SELECT [Expiry] FROM [RateLimterCache] WHERE [CounterId] = @counterId));
            IF @ttl <= 0
                SET @ttl = @timeout;

            UPDATE [RateLimterCache] 
            SET   [Key]       = CAST(@count AS VARCHAR(MAX)),
                  [Count]     = @count,
                  [Expiry]    = DATEADD(second, @ttl, GETUTCDATE()) 
            WHERE [CounterId] = @counterId;
        END
        ELSE
        BEGIN
            SET @count = @delta;
            INSERT INTO [RateLimterCache] (
                [CounterId],
                [Count], 
                [Key],
                [Expiry]) 
            VALUES (
                @counterId, 
                @count,
                @key, 
                DATEADD(second, @timeout, GETUTCDATE()));
        END

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        ROLLBACK TRANSACTION;
        THROW;
    END CATCH

    SELECT @count;
END
