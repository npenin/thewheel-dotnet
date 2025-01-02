using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using TheWheel.ETL.Contracts;

namespace TheWheel.ETL.Providers
{
    public partial class Csv : IDataReceiver<CsvReceiverOptions>
    {
        private ITransport<Stream> receiverTransport;

        public static async Task<IDataReceiver<CsvReceiverOptions>> To<TTransport>(string connectionString, CancellationToken token, params KeyValuePair<string, object>[] parameters)
            where TTransport : ITransport<Stream>, new()
        {
            var transport = new TTransport();
            await transport.InitializeAsync(connectionString, token, parameters);
            return new Csv(transport);
        }

        private Csv(ITransport<Stream> transport) : this()
        {
            receiverTransport = transport;
        }

        public async Task ReceiveAsync(IDataProvider provider, CsvReceiverOptions options, CancellationToken token)
        {
            if (options.Transport != null)
                receiverTransport = options.Transport;
            if (options.Separator == Separator.Guess)
                throw new NotSupportedException("Guess separator is not supported when writing");
            var separatorChar = Csv.GetSeparatorChar(options.Separator);
            using (var reader = await provider.ExecuteReaderAsync(token))
            {
                using (var targetStream = await receiverTransport.GetStreamAsync(token))
                {
                    using (var writer = new StreamWriter(targetStream))
                    {
                        var hasRecord = reader.Read();
                        if (options.SkipLines != null)
                        {
                            for (var i = 0; i < options.SkipLines.Length; i++)
                            {
                                await writer.WriteAsync(options.SkipLines[i]);
                                writer.Write('\n');
                            }
                        }
                        if (options.FirstLineHeader && hasRecord)
                        {
                            for (var i = 0; i < reader.FieldCount; i++)
                            {
                                if (i > 0)
                                    await writer.WriteAsync(separatorChar);

                                await writer.WriteAsync('"');
                                await writer.WriteAsync(reader.GetName(i).Replace(@"""", @""""""));
                                await writer.WriteAsync('"');
                            }
                        }
                        if (hasRecord)
                            do
                            {
                                writer.Write('\n');
                                for (var i = 0; i < reader.FieldCount; i++)
                                {
                                    if (i > 0)
                                        await writer.WriteAsync(separatorChar);

                                    if (options.formatters != null && options.formatters[i] != null)
                                        await writer.WriteAsync(options.formatters[i](reader.GetValue(i)));
                                    else
                                    {
                                        switch (Type.GetTypeCode(reader.GetFieldType(i)))
                                        {
                                            case TypeCode.Empty:
                                            case TypeCode.DBNull:
                                                break;
                                            case TypeCode.Object:
                                                await writer.WriteAsync('"');
                                                var value = reader.GetValue(i);
                                                if (value != null && value != DBNull.Value)
                                                    await writer.WriteAsync(value.ToString().Replace(@"""", @""""""));
                                                await writer.WriteAsync('"');
                                                break;
                                            case TypeCode.Boolean:
                                                await writer.WriteAsync(reader.GetBoolean(i).ToString());
                                                break;
                                            case TypeCode.Char:
                                                if (reader.GetChar(i) == '"')
                                                    await writer.WriteAsync(@"""""");
                                                else
                                                    await writer.WriteAsync(reader.GetChar(i));
                                                break;
                                            case TypeCode.Byte:
                                                writer.Write(reader.GetByte(i));
                                                break;
                                            case TypeCode.SByte:
                                            case TypeCode.Int16:
                                                writer.Write(reader.GetInt16(i));
                                                break;
                                            case TypeCode.UInt16:
                                            case TypeCode.Int32:
                                                writer.Write(reader.GetInt32(i));
                                                break;
                                            case TypeCode.UInt32:
                                            case TypeCode.Int64:
                                            case TypeCode.UInt64:
                                                writer.Write(reader.GetInt64(i));
                                                break;
                                            case TypeCode.Single:
                                                writer.Write(reader.GetFloat(i));
                                                break;
                                            case TypeCode.Double:
                                                writer.Write(reader.GetDouble(i));
                                                break;
                                            case TypeCode.Decimal:
                                                writer.Write(reader.GetDecimal(i));
                                                break;
                                            case TypeCode.DateTime:
                                                await writer.WriteAsync(reader.GetDateTime(i).ToString("u"));
                                                break;
                                            case TypeCode.String:
                                                await writer.WriteAsync('"');
                                                await writer.WriteAsync(reader.GetString(i).Replace(@"""", @""""""));
                                                await writer.WriteAsync('"');
                                                break;
                                        }
                                    }
                                }
                            }
                            while (reader.Read() && !token.IsCancellationRequested);
                    }
                }
            }
        }
    }

    public class CsvReceiverOptions : CsvOptions, IConfigurableAsync<ITransport<Stream>, CsvReceiverOptions>
    {
        public CsvReceiverOptions()
        {

        }

        public CsvReceiverOptions(CsvOptions options)
      : base(options)
        {
        }
        public CsvReceiverOptions(ITransport<Stream> transport, CsvOptions options)
      : base(transport, options)
        {
        }

        public Func<object, string>[] formatters;

        public new Task<CsvReceiverOptions> Configure(ITransport<Stream> transport, CancellationToken token)
        {
            return Task.FromResult(new CsvReceiverOptions(transport, this));
        }

        async Task<CsvReceiverOptions> IConfigurableAsync<ITransport<Stream>, CsvReceiverOptions>.Configure(ITransport<Stream> options, CancellationToken token)
        {
            await this.Configure(options, token);
            return this;
        }
    }

}
