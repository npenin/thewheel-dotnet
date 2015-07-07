ALTER PROCEDURE [dbo].[SendBrokerMessage] 
	@FromService SYSNAME,
	@MessageType SYSNAME,
	@MessageBody nvarchar(max),
	@IsOneWay bit,
	@group uniqueidentifier=null
AS
BEGIN
  SET NOCOUNT ON;
 
  DECLARE @conversation_handles TABLE(handle UNIQUEIDENTIFIER);
  DECLARE @conversation_handle UNIQUEIDENTIFIER;
  DECLARE @ToServices TABLE([To] sysname, [Contract] sysname)
  DECLARE @InQueue sysname;

  IF @group IS NULL
  BEGIN
	SET @group=NEWID()
  END

  BEGIN TRANSACTION;

  SELECT @InQueue= sq.name FROM sys.services s
INNER JOIN sys.service_queues sq ON s.service_queue_id=sq.object_id
WHERE s.name=@FromService
	

  INSERT INTO @ToServices([To], [Contract])
  SELECT s.name as [Service], sc.name as [Contract] FROM sys.services s
INNER JOIN sys.service_queues sq ON s.service_queue_id=sq.object_id
INNER JOIN sys.service_contract_usages scu ON scu.service_id=s.service_id
INNER JOIN sys.service_contracts sc ON scu.service_contract_id=sc.service_contract_id
INNER JOIN sys.service_contract_message_usages scmu ON scmu.service_contract_id=scu.service_contract_id
INNER JOIN sys.service_message_types smt ON smt.message_type_id=scmu.message_type_id
WHERE smt.name=@MessageType

 DECLARE @count int =@@ROWCOUNT;
 DECLARE @total int =@count;

 DECLARE @ToService sysname;
 DECLARE @Contract sysname;

 WHILE (@count>0)
 BEGIN
	SELECT TOP 1 @ToService=[To], @Contract=[Contract] FROM @ToServices
	DELETE FROM @ToServices WHERE [To]=@ToService AND [Contract]=@Contract
	SET @count=@count-1

	  BEGIN DIALOG CONVERSATION @conversation_handle
		FROM SERVICE @FromService
		TO SERVICE @ToService
		ON CONTRACT @Contract
		WITH
			RELATED_CONVERSATION_GROUP=@group,
			ENCRYPTION = OFF;
 
	  INSERT INTO @conversation_handles VALUES(@conversation_handle);

	  SEND ON CONVERSATION @conversation_handle
		MESSAGE TYPE @MessageType(@MessageBody);
 END
  COMMIT TRANSACTION;
  
	IF @IsOneWay=0
	BEGIN
		DECLARE @messages TABLE(message_type_name sysname, message_body nvarchar(max));

		WHILE(@count<@total)
		BEGIN
			DECLARE @receive nvarchar(max)='WAITFOR (RECEIVE TOP (@total) message_type_name, CONVERT(nvarchar(max), message_body) as message_body FROM [' + @InQueue+'] WHERE conversation_group_id=@group), TIMEOUT 30000';
			print @receive;
			INSERT INTO @messages
			exec sp_executesql @receive, N'@total int, @group uniqueidentifier', @total=@total, @group=@group
			SET @count=@@ROWCOUNT+@count
		END

		SELECT * FROM @messages
	END

	RETURN @total;
END