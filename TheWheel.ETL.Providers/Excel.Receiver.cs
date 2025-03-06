using DocumentFormat.OpenXml.Packaging;
using DocumentFormat.OpenXml.Spreadsheet;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using TheWheel.ETL.Contracts;
using TheWheel.Domain;
using DocumentFormat.OpenXml;
using System.Threading;
using System.IO;
using DocumentFormat.OpenXml.Vml.Spreadsheet;
using DocumentFormat.OpenXml.Drawing.Charts;

namespace TheWheel.ETL.Providers
{
    public partial class Excel : IDataReceiver<ExcelReceiverOptions>
    {
        private ITransport<Stream> receiverTransport;
        public static async Task<IDataReceiver<ExcelReceiverOptions>> To<TTransport>(string connectionString, CancellationToken token, params KeyValuePair<string, object>[] parameters)
            where TTransport : ITransport<Stream>, new()
        {
            var transport = new TTransport();
            await transport.InitializeAsync(connectionString, token, parameters);
            return new Excel(transport);
        }

        private Excel(ITransport<Stream> transport) : this()
        {
            receiverTransport = transport;
        }

        public async Task ReceiveAsync(IDataProvider provider, ExcelReceiverOptions options, CancellationToken token)
        {
            if (options.Transport != null)
                receiverTransport = options.Transport;
            ArgumentNullException.ThrowIfNull(receiverTransport, nameof(options.Transport));

            using (var stream = await receiverTransport.GetStreamAsync(token))
            using (this.doc = SpreadsheetDocument.Create(stream, SpreadsheetDocumentType.Workbook))
            {
                // Add a WorkbookPart to the document.
                WorkbookPart workbookPart = doc.AddWorkbookPart();
                workbookPart.Workbook = new Workbook();

                // Add a WorksheetPart to the WorkbookPart.
                WorksheetPart worksheetPart = workbookPart.AddNewPart<WorksheetPart>();
                var sheetData = new SheetData();
                worksheetPart.Worksheet = new Worksheet(sheetData);
                var sharedString = workbookPart.AddNewPart<SharedStringTablePart>();
                sharedString.SharedStringTable = new SharedStringTable();
                var sst = 0;

                // Add Sheets to the Workbook.
                Sheets sheets = workbookPart.Workbook.AppendChild(new Sheets());

                // Append a new worksheet and associate it with the workbook.
                Sheet sheet = new Sheet()
                {
                    Id = workbookPart.GetIdOfPart(worksheetPart),
                    SheetId = 1,
                    Name = options.SpreadsheetName ?? "Sheet 2"
                };
                sheets.Append(sheet);

                var reader = await provider.ExecuteReaderAsync(token);
                uint rowIndex = 0;
                uint maxCols = 0;
                List<string> headers = new();

                if (reader.Read())
                {
                    var header = new Row() { RowIndex = ++rowIndex };
                    for (var i = 0; i < reader.FieldCount; i++)
                    {
                        headers.Add(reader.GetName(i));
                        sharedString.SharedStringTable.AppendChild(new SharedStringItem(new Text(headers[i])));
                        var cell = new Cell
                        {
                            CellReference = $"{GetColumn(i + 1)}{rowIndex}",
                            DataType = CellValues.SharedString,
                            CellValue = new CellValue(sst++)
                        };
                        header.Append(cell);
                    }
                    sheetData.Append(header);

                    do
                    {
                        maxCols = Math.Max(maxCols, (uint)reader.FieldCount);
                        var row = new Row() { RowIndex = ++rowIndex };
                        for (var i = 0; i < reader.FieldCount; i++)
                        {
                            sharedString.SharedStringTable.AppendChild(new SharedStringItem(new Text(reader[i].ToString())));
                            var cell = new Cell
                            {
                                CellReference = $"{GetColumn(i + 1)}{rowIndex}",
                                DataType = CellValues.SharedString,
                                CellValue = new CellValue(sst++)
                            };
                            row.Append(cell);
                        }
                        sheetData.Append(row);
                    }
                    while (reader.Read() && !token.IsCancellationRequested);
                    if (options.TableName != null)
                        DefineTable(worksheetPart, options.TableName, 1, rowIndex, 1, headers);
                    worksheetPart.Worksheet.Save();
                    sharedString.SharedStringTable.Save();
                    doc.Save();
                }
            }
        }
        public static Table DefineTable(WorksheetPart worksheetPart, string tableName, int rowMin, uint rowMax, uint colMin, List<string> headers)
        {
            TableDefinitionPart tableDefinitionPart = worksheetPart.AddNewPart<TableDefinitionPart>("rId" + (worksheetPart.TableDefinitionParts.Count() + 1));
            var tableNo = (uint)worksheetPart.TableDefinitionParts.Count();

            string reference = GetColumn(colMin) + rowMin + ":" + GetColumn(headers.Count) + rowMax;

            Table table = new Table() { Id = tableNo, Name = "Table" + tableNo, DisplayName = tableName, Reference = reference, TotalsRowShown = false };
            AutoFilter autoFilter = new AutoFilter() { Reference = reference };

            TableColumns tableColumns = new TableColumns() { Count = (uint)headers.Count - colMin + 1 };
            for (uint i = 0; i < (headers.Count - colMin + 1); i++)
            {
                tableColumns.Append(new TableColumn() { Id = colMin + i, Name = headers[(int)i] });
            }

            TableStyleInfo tableStyleInfo = new TableStyleInfo() { Name = "TableStyleMedium2", ShowFirstColumn = false, ShowLastColumn = false, ShowRowStripes = true, ShowColumnStripes = false };

            table.Append(autoFilter);
            table.Append(tableColumns);
            table.Append(tableStyleInfo);

            tableDefinitionPart.Table = table;

            var tableParts = worksheetPart.Worksheet.GetFirstChild<TableParts>();
            if (tableParts is null)
            {
                tableParts = new TableParts();
                tableParts.Count = 0;
                worksheetPart.Worksheet.Append(tableParts);
            }

            tableParts.Count += 1;
            TablePart tablePart = new TablePart() { Id = "rId" + tableNo };

            tableParts.Append(tablePart);

            return table;
        }

    }



    public class ExcelReceiverOptions : IConfigurableAsync<ITransport<Stream>, ExcelReceiverOptions>, ITransportable<ITransport<Stream>>
    {
        public string SpreadsheetName { get; set; }

        public ITransport<Stream> Transport { get; set; }
        public string TableName { get; set; }

        public ExcelReceiverOptions()
        {

        }

        public ExcelReceiverOptions(ExcelReceiverOptions options)
        : this(options.Transport, options)
        {
        }

        public ExcelReceiverOptions(ITransport<Stream> transport, ExcelReceiverOptions other)
        {
            this.Transport = transport;
            this.SpreadsheetName = other.SpreadsheetName;
        }
        public Task<ExcelReceiverOptions> Configure(ITransport<Stream> options, CancellationToken token)
        {
            return Task.FromResult(new ExcelReceiverOptions(options, this));
        }
    }
}
