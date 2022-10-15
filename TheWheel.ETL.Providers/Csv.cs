using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using TheWheel.ETL.Contracts;

namespace TheWheel.ETL.Providers
{
    public partial class Csv : DataReader, IConfigurableAsync<CsvOptions, IDataReader>
    {
        public static async Task<DataProvider<Csv, CsvOptions, ITransport<Stream>>> From<TTransport>(string connectionString, CancellationToken token, params KeyValuePair<string, object>[] parameters)
            where TTransport : ITransport<Stream>, new()
        {
            var provider = new DataProvider<Csv, CsvOptions, ITransport<Stream>>();
            provider.Initialize(new TTransport());
            await provider.Transport.InitializeAsync(connectionString, token, parameters);
            return provider;
        }

        private char[] buffer;
        private CsvOptions options;
        public StreamReader BaseStream;

        public Csv() : base("TheWheel.ETL.Providers.Csv")
        {
        }

        public async Task<IDataReader> Configure(CsvOptions options, CancellationToken token)
        {
            buffer = new char[options.BufferSize];

            this.options = options;
            this.BaseStream = new StreamReader(await options.Transport.GetStreamAsync(token));
            return this;
        }

        public override int Depth => 0;

        public override bool IsClosed => BaseStream.EndOfStream;

        public override int RecordsAffected => 0;

        public override void Close()
        {
            BaseStream.Close();
        }

        public override DataTable GetSchemaTable()
        {
            throw new NotSupportedException();
        }

        public override bool NextResult()
        {
            return Read();
        }

        public override bool Read()
        {
            if (options.Header == null)
            {
                if (options.SkipLines != null)
                    for (var i = 0; i < options.SkipLines.Length; i++)
                    {
                        options.SkipLines[i] = ReadLine(false);
                    }
                options.Header = ParseLine(ReadLine(true), ref options.Separator);
            }
            var values = ParseLine(ReadLine(true), ref options.Separator);
            if (values == null)
                Current = null;
            else
                Current = new DataRecord(values, options.Header);
            return Current != null;
        }

        int blocIndex = 0;
        int bufferIndex = 0;
        int readLength;
        char[] lineSeparator = new[] { '\n' };

        private string ReadLine(bool ignoreEmptyLine)
        {
            if (BaseStream.EndOfStream && readLength == 0)
                return null;
            StringBuilder sb = new StringBuilder();
            var lineStart = bufferIndex;
            if (bufferIndex == 0)
            {
                readLength = BaseStream.ReadBlock(buffer, bufferIndex, buffer.Length);
                blocIndex += readLength;
            }
            bool escaped = false;
            while (bufferIndex <= readLength)
            {
                if (bufferIndex < readLength)
                {
                    if (buffer[bufferIndex] == '"')
                        escaped = !escaped;

                    if (!escaped && lineSeparator.Contains(buffer[bufferIndex]))
                    {
                        if (lineStart == 0 && bufferIndex == 0 && blocIndex > 0 && sb.Length > 0)
                        {
                            bufferIndex = ++lineStart;
                            return sb.ToString();
                        }
                        if (lineStart == bufferIndex)
                        {
                            if (ignoreEmptyLine)
                                continue;
                            bufferIndex = ++lineStart;
                            return string.Empty;
                        }

                        sb.Append(buffer, lineStart, bufferIndex - lineStart);
                        bufferIndex++;
                        return sb.ToString();
                    }

                    bufferIndex++;
                }
                if (bufferIndex == readLength)
                {
                    sb.Append(buffer, lineStart, bufferIndex - lineStart);
                    bufferIndex = 0;
                    lineStart = 0;
                    readLength = BaseStream.ReadBlock(buffer, bufferIndex, buffer.Length);
                    if (readLength == 0 && BaseStream.EndOfStream)
                    {
                        if (sb.Length == 0)
                            return null;
                        return sb.ToString();
                    }
                    blocIndex += readLength;
                }
            }

            throw new Exception("Unknown state");
        }

        private static string[] ParseLine(string line, ref Separator separator)
        {
            var columns = new List<string>();
            if (line == null)
                return null;

            if (separator == Separator.Guess)
                GetSeparator(line, ref separator);

            char separatorChar = GetSeparatorChar(separator);


            var escaped = false;
            var offset = 0;

            char[] value = new char[line.Length];

            for (var i = 0; i < line.Length; i++)
            {
                if (line[i] == '"')
                    if (i < line.Length - 1 && line[i + 1] == '"')
                        i++;
                    else
                    {
                        escaped = !escaped;
                        i++;
                    }
                if (!escaped && (i == line.Length || line[i] == separatorChar))
                {
                    columns.Add(new string(value, 0, offset));
#if NET5_0
                    Array.Fill(value, '\0', 0, offset + 1);
#else
                    for (int j = 0; j < offset + 1; j++)
                        value[j] = '\0';
#endif
                    offset = 0;
                }
                else if (i != line.Length && line[i] != '\r')
                    value[offset++] = line[i];
            }
            if (offset > 0)
                columns.Add(new string(value, 0, offset));

            return columns.ToArray();
        }

        public static char GetSeparatorChar(Separator separator)
        {
            if (separator == Separator.Guess)
                throw new NotSupportedException($"{separator} is not a supported character");

            return (char)separator;
        }

        private static void GetSeparator(string line, ref Separator separator)
        {
            var commaChances = line.Length - line.Replace(",", "").Length;
            var semiColonChances = line.Length - line.Replace(";", "").Length;
            var colonChances = line.Length - line.Replace(":", "").Length;
            var pipeChances = line.Length - line.Replace("|", "").Length;
            if (commaChances > semiColonChances && commaChances > colonChances && commaChances > pipeChances)
            {
                separator = Separator.Comma;
            }
            else if (semiColonChances > commaChances && semiColonChances > colonChances && semiColonChances > pipeChances)
            {
                separator = Separator.SemiColon;
            }
            else if (colonChances > commaChances && colonChances > semiColonChances && colonChances > pipeChances)
            {
                separator = Separator.Colon;
            }
            else if (pipeChances > commaChances && pipeChances > semiColonChances && pipeChances > colonChances)
            {
                separator = Separator.Pipe;
            }
            if (separator == Separator.Guess)
                throw new InvalidOperationException("Could not recognize separator in " + line);

        }
    }

}
