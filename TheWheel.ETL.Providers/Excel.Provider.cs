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

namespace TheWheel.ETL.Providers
{
    public partial class Excel : IDataReader//, IConfigurable<CsvOptions, Task<IDataReader>>
    {
        public Excel()
        {
        }

        private SpreadsheetDocument doc;
        private IEnumerator<Row> data;
        private Bag<string, string> header;

        private static Regex reference = new Regex("^(?<column>[A-Z]+)(?<row>[0-9]+)$");
        private SharedStringTablePart strings;
        private System.IO.Stream stream;
        private List<string> headerNames = new List<string>();

        public int Depth => 0;

        public bool IsClosed => doc != null;

        public int RecordsAffected => -1;

        public int FieldCount => throw new NotImplementedException();

        public object this[string name] => throw new NotImplementedException();

        public object this[int i] => throw new NotImplementedException();


        private static WorksheetPart GetWorksheetPartByName(SpreadsheetDocument document, string sheetName)
        {
            IEnumerable<Sheet> sheets = document.WorkbookPart.Workbook.GetFirstChild<Sheets>().Elements<Sheet>().Where(s => s.Name == sheetName);

            if (sheets.Count() == 0)
            {
                // The specified worksheet does not exist.

                return null;
            }

            string relationshipId = sheets.First().Id.Value;
            WorksheetPart worksheetPart = (WorksheetPart)
                 document.WorkbookPart.GetPartById(relationshipId);
            return worksheetPart;

        }


        public void QueryInternal(string query)
        {
            int sheetIndex;
            Sheet sheet;
            if (!int.TryParse(query, out sheetIndex))
                sheet = doc.WorkbookPart.Workbook.Sheets.Elements<Sheet>().First(s => s.Name == query);
            else
                sheet = doc.WorkbookPart.Workbook.Sheets.Elements<Sheet>().Where((s, i) => i == sheetIndex).First();

            var part = (WorksheetPart)doc.WorkbookPart.GetPartById(sheet.Id.Value);

            this.data = part.Worksheet.GetFirstChild<SheetData>().Elements<Row>().GetEnumerator();
            this.strings = doc.WorkbookPart.GetPartsOfType<SharedStringTablePart>().FirstOrDefault();
            if (this.Read())
            {
                var thead = this.data.Current.Elements<Cell>().ToArray();
                this.header = new Bag<string, string>(thead.Length);
                for (int i = 0; i < thead.Length; i++)
                {
                    var text = GetString(i);
                    if (text != null)
                    {
                        header.Add(text, thead[i].CellReference.Value);
                        headerNames.Add(text);
                    }
                }
            }
        }

        public bool Read()
        {
            return this.data != null && this.data.MoveNext();
        }

        public void Dispose()
        {
            if (this.doc != null)
            {
                //this.doc.Close();
                this.doc.Dispose();
            }
            if (this.stream != null)
            {
                this.stream.Close();
                this.stream.Dispose();
            }

            this.doc = null;
        }

        public void Initialize(System.IO.Stream s)
        {
            this.doc = SpreadsheetDocument.Open(stream = s, false);
        }

        public void Close()
        {
            if (this.doc != null)
                this.doc.Dispose();
            this.doc = null;
        }

        public System.Data.DataTable GetSchemaTable()
        {
            throw new NotImplementedException();
        }

        public bool NextResult()
        {
            return false;
        }

        private static string GetColumn(int i)
        {
            var length = (int)Math.Ceiling(i / (decimal)columns.Length);
            var column = new char[length];
            for (int j = 0; j < length; j++)
            {
                column[j] = columns[i % columns.Length];
                i = (i - columns[j]) / columns.Length;
            }
            return new string(column);
        }

        private static string GetColumn(uint i)
        {
            var length = (int)Math.Ceiling(i / (decimal)columns.Length);
            var column = new char[length];
            for (int j = 0; j < length; j++)
            {
                column[j] = columns[(int)(i % columns.Length)];
                i = (uint)((i - columns[j]) / columns.Length);
            }
            return new string(column);
        }

        private Cell GetCell(string column)
        {
            return data.Current.Elements<Cell>().FirstOrDefault(c => c.CellReference == column + data.Current.RowIndex);
        }
        private Cell GetCell(int i)
        {
            return GetCell(GetColumn(i) + data.Current.RowIndex);
        }

        const string columns = "$ABCDEFGHIJKLMNOPQRSTUVWYZ";

        public bool GetBoolean(int i)
        {
            var cell = GetCell(i);
            return cell.CellValue.Text != "0";
        }

        public byte GetByte(int i)
        {
            var cell = GetCell(i);
            return Convert.ToByte(cell.CellValue.Text);
        }

        public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
        {
            throw new NotImplementedException();
        }

        public char GetChar(int i)
        {
            var cell = GetCell(i);
            return Convert.ToChar(cell.CellValue.Text);
        }

        public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
        {
            throw new NotImplementedException();
        }

        public IDataReader GetData(int i)
        {
            throw new NotImplementedException();
        }

        public string GetDataTypeName(int i)
        {
            throw new NotImplementedException();
        }

        public DateTime GetDateTime(int i)
        {
            var cell = GetCell(i);
            return DateTime.FromOADate(double.Parse(cell.CellValue.Text));
        }

        public decimal GetDecimal(int i)
        {
            var cell = GetCell(i);
            return Convert.ToDecimal(cell.CellValue.Text);
        }

        public double GetDouble(int i)
        {
            var cell = GetCell(i);
            return Convert.ToDouble(cell.CellValue.Text);
        }

        public TypeCode GetFieldTypeCode(string reference)
        {
            var cell = GetCell(reference);
            switch (((IEnumValue)cell.DataType.Value).Value)
            {
                case "b":
                    return TypeCode.Boolean;
                case "d":
                    return TypeCode.DateTime;
                case "e":
                    return TypeCode.Empty;
                case "n":
                    return TypeCode.Double;
                case "inlineStr":
                case "str":
                case "s":
                    return TypeCode.String;
            }
            return TypeCode.DBNull;
        }

        public Type GetFieldType(int i)
        {
            var cell = GetCell(i);
            switch (((IEnumValue)cell.DataType.Value).Value)
            {
                case "b":
                    return typeof(bool);
                case "d":
                    return typeof(DateTime);
                case "e":
                    return null;
                case "n":
                    return typeof(double);
                case "inlineStr":
                case "str":
                case "s":
                    return typeof(string);
            }
            return typeof(DBNull);
        }

        public float GetFloat(int i)
        {
            var cell = GetCell(i);
            return Convert.ToSingle(cell.CellValue.Text);
        }

        public Guid GetGuid(int i)
        {
            return new Guid(GetString(i));
        }

        public short GetInt16(int i)
        {
            var cell = GetCell(i);
            return Convert.ToInt16(cell.CellValue.Text);
        }

        public int GetInt32(int i)
        {
            var cell = GetCell(i);
            return Convert.ToInt32(cell.CellValue.Text);
        }

        public long GetInt64(int i)
        {
            var cell = GetCell(i);
            return Convert.ToInt64(cell.CellValue.Text);
        }

        public string GetName(int i)
        {
            var cell = GetCell(i);
            return cell.CellReference.Value;
        }

        public int GetOrdinal(string name)
        {
            var cellRef = name;
            if (!header.TryGetValue(name, out cellRef))
                cellRef = name;

            cellRef = reference.Match(cellRef).Groups["column"].Value;
            var ordinal = 0;
            for (int j = cellRef.Length; j > 0; j--)
            {
                ordinal += (int)Math.Pow(columns.Length - 1, cellRef.Length - j) * (columns.IndexOf(cellRef[j - 1]));
            }
            return ordinal;
        }

        public string GetString(int i)
        {
            throw new NotImplementedException();
        }

        public object GetValue(int i)
        {
            throw new NotImplementedException();
        }

        public int GetValues(object[] values)
        {
            throw new NotImplementedException();
        }

        public bool IsDBNull(int i)
        {
            throw new NotImplementedException();
        }
    }
}
