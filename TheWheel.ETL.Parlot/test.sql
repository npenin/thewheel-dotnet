DECLARE @lorem int=1;
DECLARE @ipsum string='ipsum';

SELECT @lorem as [test], @ipsum as [ipsum], [column2] as [column0], *, 2 as [test2]
INTO Csv<FileWrite>('TheWheel.ETL.Tests/test-parlotoutput.csv', new CsvReceiverOptions { SkipLines = null, Separator = Separator.Colon })
FROM Csv<FileRead>('TheWheel.ETL.Tests/test.csv', new CsvOptions{ SkipLines=new string[4], Separator = Separator.Comma})