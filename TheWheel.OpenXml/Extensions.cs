using DocumentFormat.OpenXml.Packaging;
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TheWheel.Domain;
using TheWheel.Lambda;
using System.Linq.Expressions;

namespace TheWheel.OpenXml
{
    public class Extensions
    {
        public static void Export<T>(Stream s, IEnumerable<T> items, string undefinedColumnName, Func<string, string> Translate)
        {
            Export(s, typeof(T), items, undefinedColumnName, Translate);
        }

        public static void Export(Stream s, Type type, IEnumerable items, string undefinedColumnName, Func<string, string> Translate)
        {
            var cols = type.GetProperties().Select((p, i) => new Column() { TranslatedName = Translate(p.Name), Code = p.Name, Name = p.Name, Order = i }).ToList();
            Export(s, items, cols, undefinedColumnName);
        }

        public static void Export(Stream s, IEnumerable items, IEnumerable<IColumn> cols, string undefinedColumnName)
        {
            var doc = SpreadsheetDocument.Create(s, DocumentFormat.OpenXml.SpreadsheetDocumentType.Workbook);
            doc.AddWorkbookPart();
            doc.WorkbookPart.Workbook = new DocumentFormat.OpenXml.Spreadsheet.Workbook();
            var worksheetPart = doc.WorkbookPart.AddNewPart<WorksheetPart>();
            var data = new DocumentFormat.OpenXml.Spreadsheet.SheetData();
            worksheetPart.Worksheet = new DocumentFormat.OpenXml.Spreadsheet.Worksheet();
            doc.WorkbookPart.Workbook.AppendChild(new DocumentFormat.OpenXml.Spreadsheet.Sheets());
            var sheet = new DocumentFormat.OpenXml.Spreadsheet.Sheet()
            {
                Id = doc.WorkbookPart.GetIdOfPart(worksheetPart),
                SheetId = 1,
                Name = "Export"
            };
            doc.WorkbookPart.Workbook.Sheets.AppendChild(sheet);
            var row = new DocumentFormat.OpenXml.Spreadsheet.Row();
            data.AppendChild(row);
            var tableDef = worksheetPart.AddNewPart<TableDefinitionPart>("rId1");
            tableDef.Table = new DocumentFormat.OpenXml.Spreadsheet.Table();
            tableDef.Table.Name = "Export";
            tableDef.Table.DisplayName = "Table1";
            tableDef.Table.Id = 1;
            //tableDef.Table.HeaderRowCount = 1;
            tableDef.Table.Reference = GetReference(1, 1) + ":" + GetReference(cols.Count(), items.Count() + 1);
            tableDef.Table.AutoFilter = new DocumentFormat.OpenXml.Spreadsheet.AutoFilter() { Reference = tableDef.Table.Reference };
            tableDef.Table.TableColumns = new DocumentFormat.OpenXml.Spreadsheet.TableColumns();
            tableDef.Table.TableColumns.Count = (uint)cols.Count();
            worksheetPart.Worksheet.SheetDimension = new DocumentFormat.OpenXml.Spreadsheet.SheetDimension() { Reference = tableDef.Table.Reference };
            DocumentFormat.OpenXml.Spreadsheet.Columns columns = new DocumentFormat.OpenXml.Spreadsheet.Columns();
            worksheetPart.Worksheet.AppendChild(columns);
            List<double> columnSize = new List<double>();
            var g = System.Drawing.Graphics.FromImage(new System.Drawing.Bitmap(50, 50));
            var maxCharWidth = g.MeasureString("0", new System.Drawing.Font("Calibri", 11)).Width;
            foreach (var column in cols.OrderBy(c => c.Order))
            {
                if (column.TranslatedName == undefinedColumnName)
                    column.TranslatedName = undefinedColumnName + column.Id;
                row.AppendChild(new DocumentFormat.OpenXml.Spreadsheet.Cell() { DataType = DocumentFormat.OpenXml.Spreadsheet.CellValues.String, CellValue = new DocumentFormat.OpenXml.Spreadsheet.CellValue(column.TranslatedName) });
                tableDef.Table.TableColumns.AppendChild(new DocumentFormat.OpenXml.Spreadsheet.TableColumn() { Name = column.TranslatedName, Id = (uint)column.Id });
                columnSize.Add(Math.Truncate((column.TranslatedName.Length * maxCharWidth + 55) / maxCharWidth * 256) / 256);
            }
            tableDef.Table.TableStyleInfo = new DocumentFormat.OpenXml.Spreadsheet.TableStyleInfo() { Name = "TableStyleMedium2", ShowRowStripes = true, ShowFirstColumn = false, ShowLastColumn = false, ShowColumnStripes = false };
            DocumentFormat.OpenXml.Spreadsheet.TableParts tableParts = new DocumentFormat.OpenXml.Spreadsheet.TableParts { Count = 1 };
            tableParts.Append(new DocumentFormat.OpenXml.Spreadsheet.TablePart { Id = "rId1" });
            foreach (var item in items)
            {
                row = new DocumentFormat.OpenXml.Spreadsheet.Row();
                data.AppendChild(row);
                int iCol = 0;
                foreach (var column in cols)
                {
                    var value = item.Property(column.Code);
                    string stringValue = null;
                    if (value != null)
                    {
                        stringValue = value.ToString();
                        columnSize[iCol] = Math.Max(columnSize[iCol], Math.Min(Math.Truncate((stringValue.Length * maxCharWidth + 5) / maxCharWidth * 256) / 256, 100));
                    }

                    var dataType = GetDataType(value);
                    row.AppendChild(new DocumentFormat.OpenXml.Spreadsheet.Cell() { DataType = dataType, CellValue = new DocumentFormat.OpenXml.Spreadsheet.CellValue(stringValue) });
                    iCol++;
                }
            }
            for (uint i = 0; i < columnSize.Count; i++)
            {

                columns.Append(new DocumentFormat.OpenXml.Spreadsheet.Column
                {
                    Min = i + 1,
                    Max = i + 1,
                    BestFit = true,
                    Width = columnSize[(int)i],
                    CustomWidth = true
                });
            }
            worksheetPart.Worksheet.AppendChild(data);
            worksheetPart.Worksheet.AppendChild(tableParts);
            doc.WorkbookPart.Workbook.Save();
            doc.Close();
        }


        private static DocumentFormat.OpenXml.Spreadsheet.CellValues GetDataType(object value)
        {
            return GetDataType(Convert.GetTypeCode(value));
        }

        private static DocumentFormat.OpenXml.Spreadsheet.CellValues GetDataType(TypeCode type)
        {
            switch (type)
            {
                case TypeCode.Boolean:
                    return DocumentFormat.OpenXml.Spreadsheet.CellValues.Boolean;
                case TypeCode.Byte:
                case TypeCode.Decimal:
                case TypeCode.Double:
                case TypeCode.Int16:
                case TypeCode.Int32:
                case TypeCode.Int64:
                case TypeCode.SByte:
                case TypeCode.Single:
                case TypeCode.UInt16:
                case TypeCode.UInt32:
                case TypeCode.UInt64:
                    return DocumentFormat.OpenXml.Spreadsheet.CellValues.Number;
                case TypeCode.Char:
                case TypeCode.DBNull:
                case TypeCode.Empty:
                case TypeCode.Object:
                case TypeCode.String:
                    return DocumentFormat.OpenXml.Spreadsheet.CellValues.String;
                case TypeCode.DateTime:
                    return DocumentFormat.OpenXml.Spreadsheet.CellValues.Date;
            }
            return DocumentFormat.OpenXml.Spreadsheet.CellValues.String;
        }

        private static string GetReference(int colIndex, int rowIndex)
        {
            colIndex--;
            const string columnName = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
            StringBuilder sb = new StringBuilder();
            while (colIndex >= 0)
            {
                sb.Append(columnName[colIndex % columnName.Length]);
                colIndex -= columnName.Length;
            }
            sb.Append(rowIndex);
            return sb.ToString();
        }
    }
}
