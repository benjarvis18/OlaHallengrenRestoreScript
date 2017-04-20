CREATE VIEW [Backups].[SuccessfulBackups]
WITH SCHEMABINDING
AS
	SELECT	ID AS BackupID,
			DatabaseName AS DBName,			
			CONVERT(datetime, STUFF(STUFF(STUFF(
				REPLACE(SUBSTRING(Command, CHARINDEX(CASE WHEN Command LIKE '%\LOG\%' OR Command LIKE '%\LOG_COPY_ONLY\%' THEN '.trn' -- .TRN for transaction log backups
														  ELSE '.bak' -- .BAK for DIFF and FULL
													 END, Command) - CASE WHEN (LEN(Command) - LEN(REPLACE(Command, 'DISK', ''))) / 4 > 1 THEN 18 ELSE 15 END, 15), '_', '') -- Case statement is to cope with backups that use multiple files
			, 9, 0, ' '), 12, 0, ':'), 15, 0, ':'), 103) AS ExecutionDateTime, -- Looks slightly dodgy but this allows us to use an indexed view which gives massive performance gains
			CASE WHEN Command LIKE '%\LOG\%' THEN N'LOG'
					WHEN Command LIKE '%\LOG_COPY_ONLY\%' THEN N'LOG_COPY_ONLY'
					WHEN Command LIKE '%\DIFF\%' THEN N'DIFF'
					WHEN Command LIKE '%\FULL\%' THEN N'FULL'
					WHEN Command LIKE '%\FULL_COPY_ONLY\%' THEN N'FULL_COPY_ONLY'
			END BackupType,
			SUBSTRING(Command, CHARINDEX('TO DISK', Command) + 3, LEN(Command) - CHARINDEX('TO DISK', Command) - CHARINDEX(CASE	WHEN Command LIKE '%\LOG\%'
																																		OR Command LIKE '%\LOG_COPY_ONLY\%' THEN 'nrt.' -- Last occurrence of transaction log (.TRN)
																																ELSE 'kab.' -- Last occurrence of .BAK for DIFF and FULL
																															END, REVERSE(Command))) AS FileList
	FROM	dbo.CommandLog
	WHERE	CommandType IN ( 'BACKUP_DATABASE', 'BACKUP_LOG' )
			AND EndTime IS NOT NULL
			AND ErrorNumber = 0

GO
CREATE UNIQUE CLUSTERED INDEX CIX_SuccessfulBackups_BackupID ON Backups.SuccessfulBackups(BackupId)
