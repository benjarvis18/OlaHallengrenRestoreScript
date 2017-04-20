
CREATE PROCEDURE Backups.RestoreDatabase
	@DatabaseName sysname,		
	@RestoreAsDatabaseName sysname = NULL,
	@ExecuteScripts char(1) = 'N',
	@RestoreFullAndDiffBackupOnly char(1) = 'N',
	@Replace char(1) = 'N',
	@BufferCount int = 1000,
	@StopAt datetime = NULL,
	@LogToTable char(1) = 'N'
AS 
BEGIN
	/*
		Script created by Ben Jarvis.		
		
		This script makes use of the Backups.BackupRepository table to generate / execute a database restore script. 
		The script will pick up the latest full backup and by default will restore all available differential and log backups to bring the database up to the latest available point-in-time. The script
		can perform a point in time restore by specifying a value for the @StopAt parameter (assuming @StopAt is specified as a value after the latest full backup and there are log backups available).

		The script requires the CommandLog table and CommandExecute stored procedure by Ola Hallengren that can be downloaded from https://ola.hallengren.com/downloads.html.

		Example:

		EXEC dbo.RestoreDatabase @DatabaseName = 'DBName',
								 @BackupRootFolder = 'C:\MyBackupFolder\',
								 @ExecuteScripts = 'Y',
								 @RestoreFullAndDiffBackupOnly = 'N',
								 @StopAt = NULL,
								 @LogToTable = 'Y'
	*/

	SET NOCOUNT ON 

	IF COALESCE(@DatabaseName, '') = '' 
		RAISERROR ('No @DatabaseName value provided.', 16, 1)

	IF @ExecuteScripts IS NULL OR @ExecuteScripts NOT IN ('Y', 'N')
		RAISERROR ('Invalid value passed for @ExecuteScripts.', 16, 1)

	IF @RestoreFullAndDiffBackupOnly IS NULL OR @RestoreFullAndDiffBackupOnly NOT IN ('Y', 'N')
		RAISERROR ('Invalid value passed for @RestoreFullAndDiffBackupOnly.', 16, 1)

	IF @Replace IS NULL OR @Replace NOT IN ('Y', 'N')
		RAISERROR ('Invalid value passed for @Replace.', 16, 1)

	IF @LogToTable IS NULL OR @LogToTable NOT IN ('Y', 'N')
		RAISERROR ('Invalid value passed for @LogToTable.', 16, 1)

	IF @LogToTable = 'Y' AND NOT EXISTS ( SELECT 1 FROM sys.objects O INNER JOIN sys.schemas S ON S.schema_id = O.schema_id WHERE S.name = 'dbo' AND O.name = 'CommandLog' AND O.type = 'U' )
		RAISERROR ('Parameter @LogToTable set to Y but dbo.CommandLog table does not exist. Download the script from https://ola.hallengren.com/scripts/CommandLog.sql.', 16, 1)

	IF NOT EXISTS (SELECT * FROM sys.objects O INNER JOIN sys.schemas S ON O.schema_id = S.schema_id WHERE O.type = 'P' AND S.name = 'dbo' AND O.name = 'CommandExecute')
		RAISERROR('The CommandExecute stored procedure is missing. Download https://ola.hallengren.com/scripts/CommandExecute.sql.', 16, 1)

	IF @RestoreAsDatabaseName IS NULL
		SET @RestoreAsDatabaseName = @DatabaseName

	DECLARE @Cmd nvarchar(2048)
	DECLARE @Sql nvarchar(2048)

	-- Get backup file list
	DECLARE	@FileList table
		(
			RestoreOrder int,
			BackupType varchar(20),
			BackupFileList varchar(MAX),
			RestoreStatement varchar(max)
		)

	INSERT INTO @FileList
			( RestoreOrder,
			  BackupType,
			  BackupFileList,			  
			  RestoreStatement )
		SELECT	ROW_NUMBER() OVER (ORDER BY LastLSN) AS RestoreOrder, 				
				BackupType,
				FileList,
				CASE WHEN BackupType IN ( 'FULL', 'DIFF' ) THEN 'RESTORE DATABASE [' + @RestoreAsDatabaseName + '] FROM ' + FileList + ' WITH NORECOVERY' + CASE WHEN BackupType = 'Full' AND @Replace = 'Y' THEN ', REPLACE' ELSE '' END
																																						  + COALESCE(', BUFFERCOUNT = ' + CAST(@BufferCount AS varchar(50)), '')
					 WHEN BackupType IN ( 'LOG' ) THEN 'RESTORE LOG [' + @RestoreAsDatabaseName + '] FROM ' + FileList + ' WITH NORECOVERY' + CASE WHEN @StopAt IS NOT NULL THEN ', STOPAT = ''' + CONVERT(varchar(24), @StopAt, 113) + ''''
																																				   ELSE ''
																																			  END
				END AS RestoreStatement
		FROM	Backups.BackupRepository
		WHERE	DBName = @DatabaseName
				AND (@RestoreFullAndDiffBackupOnly = 'N' OR (@RestoreFullAndDiffBackupOnly = 'Y' AND BackupType IN ('Full', 'Diff')))
				AND (BackupType <> 'LOG' OR (@StopAt IS NULL OR ExecutionDateTime <= COALESCE((SELECT TOP 1 ExecutionDateTime FROM Backups.BackupRepository WHERE DBName = @DatabaseName AND ExecutionDateTime >= @StopAt ORDER BY ExecutionDateTime), @StopAt)))
		ORDER BY LastLSN;

	INSERT	INTO @FileList
			( RestoreOrder,
			  RestoreStatement )
			SELECT	9999,
					'RESTORE DATABASE [' + @RestoreAsDatabaseName + '] WITH RECOVERY';		
					
	IF NOT EXISTS ( SELECT 1 FROM @FileList WHERE BackupType = 'Full' )	  
		RAISERROR ('No full backup found.', 16, 1)

	-- Kick people off if the db exists
	IF EXISTS ( SELECT 1 FROM sys. databases WHERE name = @RestoreAsDatabaseName )
	BEGIN
		SET @Sql = 'ALTER DATABASE [' + @RestoreAsDatabaseName + '] SET OFFLINE WITH ROLLBACK IMMEDIATE'
		PRINT @Sql

		IF @ExecuteScripts = 'Y'	
			EXEC dbo.CommandExecute @Command = @Sql, -- nvarchar(max)
									@CommandType = N'SET_DATABASE_OFFLINE', -- nvarchar(max)
									@Mode = 2, -- int
									@DatabaseName = @RestoreAsDatabaseName, -- nvarchar(max)
									@LogToTable = @LogToTable, -- nvarchar(max)
									@Execute = N'Y' -- nvarchar(max)

		SET @Sql = 'ALTER DATABASE [' + @RestoreAsDatabaseName + '] SET ONLINE'
		PRINT @Sql

		IF @ExecuteScripts = 'Y'	
			EXEC dbo.CommandExecute @Command = @Sql, -- nvarchar(max)
									@CommandType = N'SET_DATBASE_ONLINE', -- nvarchar(max)
									@Mode = 2, -- int
									@DatabaseName = @RestoreAsDatabaseName, -- nvarchar(max)
									@LogToTable = @LogToTable, -- nvarchar(max)
									@Execute = N'Y' -- nvarchar(max)
	END
	ELSE	
	BEGIN
		-- Generate move statements using default log and data folders
		DECLARE @FileListDetails TABLE
			(
			  LogicalName nvarchar(128) NOT NULL,
			  PhysicalName nvarchar(260) NOT NULL,
			  Type char(1) NOT NULL,
			  FileGroupName nvarchar(120) NULL,
			  Size numeric(20, 0) NOT NULL,
			  MaxSize numeric(20, 0) NOT NULL,
			  FileID bigint NULL,
			  CreateLSN numeric(25,0) NULL,
			  DropLSN numeric(25,0) NULL,
			  UniqueID uniqueidentifier NULL,
			  ReadOnlyLSN numeric(25,0) NULL ,
			  ReadWriteLSN numeric(25,0) NULL,
			  BackupSizeInBytes bigint NULL,
			  SourceBlockSize int NULL,
			  FileGroupID int NULL,
			  LogGroupGUID uniqueidentifier NULL,
			  DifferentialBaseLSN numeric(25,0)NULL,
			  DifferentialBaseGUID uniqueidentifier NULL,
			  IsReadOnly bit NULL,
			  IsPresent bit NULL,
			  TDEThumbprint varbinary(32) NULL
		 );

		SET @Sql = 'RESTORE FILELISTONLY FROM ' + (SELECT BackupFileList FROM @FileList WHERE BackupType = 'Full')
		
		INSERT INTO @FileListDetails
			EXEC(@Sql)
		
		-- Get default data and log paths
		DECLARE	@DefaultData nvarchar(512);
		EXEC master.dbo.xp_instance_regread N'HKEY_LOCAL_MACHINE', N'Software\Microsoft\MSSQLServer\MSSQLServer', N'DefaultData', @DefaultData OUTPUT;

		IF @DefaultData IS NULL
		BEGIN
			DECLARE	@MasterData nvarchar(512);
			EXEC master.dbo.xp_instance_regread N'HKEY_LOCAL_MACHINE', N'Software\Microsoft\MSSQLServer\MSSQLServer\Parameters', N'SqlArg0', @MasterData OUTPUT;
			SELECT	@MasterData = SUBSTRING(@MasterData, 3, 255);
			SELECT	@MasterData = SUBSTRING(@MasterData, 1, LEN(@MasterData) - CHARINDEX('\', REVERSE(@MasterData)));

			SET @DefaultData = @MasterData
		END

		DECLARE	@DefaultLog nvarchar(512);
		EXEC master.dbo.xp_instance_regread N'HKEY_LOCAL_MACHINE', N'Software\Microsoft\MSSQLServer\MSSQLServer', N'DefaultLog', @DefaultLog OUTPUT;

		IF @DefaultLog IS NULL
		BEGIN		
			DECLARE	@MasterLog nvarchar(512);
			EXEC master.dbo.xp_instance_regread N'HKEY_LOCAL_MACHINE', N'Software\Microsoft\MSSQLServer\MSSQLServer\Parameters', N'SqlArg2', @MasterLog OUTPUT;
			SELECT	@MasterLog = SUBSTRING(@MasterLog, 3, 255);
			SELECT	@MasterLog = SUBSTRING(@MasterLog, 1, LEN(@MasterLog) - CHARINDEX('\', REVERSE(@MasterLog)));
		END

		DECLARE @DefaultFileStream nvarchar(512) = @DefaultData + '\FileStream'
		EXEC master.dbo.xp_create_subdir @DefaultFileStream

		-- Generate Move Statements
		UPDATE	@FileList
		SET		RestoreStatement += LTRIM(RTRIM((SELECT	', ' + 'MOVE ''' + LogicalName + ''' TO ''' + CASE WHEN Type = 'D' THEN @DefaultData
																										   WHEN Type = 'L' THEN @DefaultLog
																										   WHEN Type = 'S' THEN @DefaultFileStream
																									  END + '\' + @RestoreAsDatabaseName + '_' + LogicalName + COALESCE('.' + CASE  WHEN FileGroupName = 'PRIMARY' THEN 'mdf'
																																													WHEN Type = 'S' THEN NULL
																																													WHEN Type = 'L' THEN 'ldf'
																																													ELSE 'ndf'
																																												END, '') + ''''
												 FROM	@FileListDetails
										  FOR	XML	PATH(''), TYPE)
			.value('.', 'NVARCHAR(MAX)')))
		WHERE	BackupType = 'Full';			
	END

	-- Run Commands	
	DECLARE cur_RestoreBackups CURSOR FAST_FORWARD READ_ONLY 
	FOR 		
		SELECT	RestoreStatement
		FROM	@FileList
		ORDER BY RestoreOrder

	OPEN cur_RestoreBackups

	FETCH NEXT FROM cur_RestoreBackups INTO @Sql

	WHILE @@FETCH_STATUS = 0 
	BEGIN
		PRINT @Sql

		IF @ExecuteScripts = 'Y'	
			EXEC dbo.CommandExecute @Command = @Sql, -- nvarchar(max)
									@CommandType = N'RESTORE_DATABASE', -- nvarchar(max)
									@Mode = 2, -- int
									@DatabaseName = @RestoreAsDatabaseName, -- nvarchar(max)
									@LogToTable = @LogToTable, -- nvarchar(max)
									@Execute = N'Y' -- nvarchar(max)

		FETCH NEXT FROM cur_RestoreBackups INTO @Sql	
	END 

	CLOSE cur_RestoreBackups
	DEALLOCATE cur_RestoreBackups
END
