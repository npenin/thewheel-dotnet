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
            if (receiverTransport == null)
                throw new ArgumentNullException(nameof(options.Transport));

            // Normalize inputs: convert single-provider mode to Sheets array
            ExcelSheetOptions[] sheetsToProcess = options.Sheets;
            if (sheetsToProcess == null || sheetsToProcess.Length == 0)
            {
                if (provider == null)
                    throw new ArgumentNullException(nameof(provider), "Either provider parameter or options.Sheets must be specified");

                // Create a single sheet option from legacy parameters
                sheetsToProcess = new ExcelSheetOptions[]
                {
                    new ExcelSheetOptions(options.SpreadsheetName ?? "Sheet 2", provider, options.TableName)
                };
            }

            // Use unified multi-provider logic for both single and multiple sheets
            await ReceiveMultipleProvidersAsync(sheetsToProcess, options, token);
        }

        /// <summary>
        /// Unified implementation for receiving data and writing to Excel worksheets.
        /// Handles both single-provider and multi-provider modes using the same code path.
        /// Each provider writes to its own sheet in a shared workbook.
        /// Similar to TreeOptions which supports multiple data matchers for hierarchical data.
        /// </summary>
        private async Task ReceiveMultipleProvidersAsync(ExcelSheetOptions[] sheets, ExcelReceiverOptions options, CancellationToken token)
        {
            using (var stream = await receiverTransport.GetStreamAsync(token))
            using (this.doc = SpreadsheetDocument.Create(stream, SpreadsheetDocumentType.Workbook))
            {
                // Add a WorkbookPart to the document.
                WorkbookPart workbookPart = doc.AddWorkbookPart();
                workbookPart.Workbook = new Workbook();

                var sharedString = workbookPart.AddNewPart<SharedStringTablePart>();
                sharedString.SharedStringTable = new SharedStringTable();
                var sst = 0;

                // Add Sheets container to the Workbook
                Sheets sheetsCollection = workbookPart.Workbook.AppendChild(new Sheets());
                var sheetId = 1u;

                // Process each provider/sheet
                for (int sheetIndex = 0; sheetIndex < sheets.Length; sheetIndex++)
                {
                    var sheetOptions = sheets[sheetIndex];
                    if (sheetOptions.Provider == null)
                        continue;

                    // Create a new WorksheetPart for this provider
                    WorksheetPart worksheetPart = workbookPart.AddNewPart<WorksheetPart>();
                    var sheetData = new SheetData();
                    worksheetPart.Worksheet = new Worksheet(sheetData);

                    // Get sheet name
                    string sheetName = sheetOptions.SheetName ?? $"Sheet{sheetId}";

                    // Create and append the sheet reference
                    Sheet sheet = new Sheet()
                    {
                        Id = workbookPart.GetIdOfPart(worksheetPart),
                        SheetId = sheetId,
                        Name = sheetName
                    };
                    sheetsCollection.Append(sheet);

                    // Execute the provider and write data to this sheet
                    var reader = await sheetOptions.Provider.ExecuteReaderAsync(token);
                    uint rowIndex = 0;
                    uint maxCols = 0;
                    List<string> headers = new List<string>();

                    if (reader.Read())
                    {
                        // Write header row
                        var header = new Row() { RowIndex = ++rowIndex };
                        for (var i = 0; i < reader.FieldCount; i++)
                        {
                            string fieldName = reader.GetName(i);
                            headers.Add(fieldName);
                            sharedString.SharedStringTable.AppendChild(new SharedStringItem(new Text(fieldName)));
                            var cell = new Cell
                            {
                                CellReference = $"{GetColumn(i + 1)}{rowIndex}",
                                DataType = CellValues.SharedString,
                                CellValue = new CellValue(sst++)
                            };
                            header.Append(cell);
                        }
                        sheetData.Append(header);

                        // Write data rows
                        do
                        {
                            maxCols = Math.Max(maxCols, (uint)reader.FieldCount);
                            var row = new Row() { RowIndex = ++rowIndex };
                            for (var i = 0; i < reader.FieldCount; i++)
                            {
                                string cellValue = reader[i]?.ToString() ?? string.Empty;
                                sharedString.SharedStringTable.AppendChild(new SharedStringItem(new Text(cellValue)));
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

                        // Define a table for this sheet if requested
                        if (sheetOptions.TableName != null)
                            DefineTable(worksheetPart, sheetOptions.TableName, 1, rowIndex, 1, headers);

                        worksheetPart.Worksheet.Save();
                    }

                    sheetId++;
                }

                sharedString.SharedStringTable.Save();
                if (doc.CanSave)
                    doc.Save();
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



    /// <summary>
    /// Represents configuration for a single sheet with an associated data provider.
    /// Inspired by TreeLeaf which represents a single path in hierarchical data structures.
    /// </summary>
    public class ExcelSheetOptions
    {
        /// <summary>
        /// Name of the sheet in the Excel workbook
        /// </summary>
        public string SheetName { get; set; }

        /// <summary>
        /// The data provider that supplies data for this sheet
        /// </summary>
        public IDataProvider Provider { get; set; }

        /// <summary>
        /// Optional name for the Excel table/range in this sheet
        /// </summary>
        public string TableName { get; set; }

        public ExcelSheetOptions()
        {
        }

        public ExcelSheetOptions(string sheetName, IDataProvider provider, string tableName = null)
        {
            SheetName = sheetName;
            Provider = provider;
            TableName = tableName;
        }
    }

    /// <summary>
    /// Options for Excel receiver supporting multiple providers (one per sheet).
    /// Similar to TreeOptions which supports multiple data matchers for hierarchical data.
    /// </summary>
    public class ExcelReceiverOptions : IConfigurableAsync<ITransport<Stream>, ExcelReceiverOptions>, ITransportable<ITransport<Stream>>
    {
        private ExcelSheetOptions[] sheets;

        /// <summary>
        /// Legacy property: name for a single spreadsheet
        /// </summary>
        public string SpreadsheetName { get; set; }

        public ITransport<Stream> Transport { get; set; }

        /// <summary>
        /// Legacy property: table name for single-provider mode
        /// </summary>
        public string TableName { get; set; }

        /// <summary>
        /// Sheet options for multi-provider mode.
        /// When set, ReceiveAsync will write each provider to its own sheet.
        /// </summary>
        public ExcelSheetOptions[] Sheets
        {
            get => sheets;
            set => sheets = value;
        }

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
            this.sheets = other.sheets;
            this.TableName = other.TableName;
        }

        /// <summary>
        /// Adds a sheet with a provider. Each provider will write to its own sheet.
        /// Similar to TreeOptions.AddMatch() which adds a new data matcher.
        /// </summary>
        public ExcelReceiverOptions AddSheet(string sheetName, IDataProvider provider, string tableName = null)
        {
            ExcelSheetOptions newSheet = new ExcelSheetOptions(sheetName, provider, tableName);

            if (sheets == null)
                sheets = new ExcelSheetOptions[1];
            else
            {
                Array.Resize(ref sheets, sheets.Length + 1);
            }
            sheets[sheets.Length - 1] = newSheet;

            return this;
        }

        /// <summary>
        /// Sets the sheets for this receiver options.
        /// </summary>
        public ExcelReceiverOptions WithSheets(params ExcelSheetOptions[] sheetOptions)
        {
            sheets = sheetOptions;
            return this;
        }

        public Task<ExcelReceiverOptions> Configure(ITransport<Stream> options, CancellationToken token)
        {
            return Task.FromResult(new ExcelReceiverOptions(options, this));
        }
    }
}
