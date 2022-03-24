using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using TheWheel.ETL.Contracts;

namespace TheWheel.ETL.Providers
{
    public partial class Json : IDataReceiver<TreeOptions>
    {
        private ITransport<Stream> receiverTransport;

        public static async Task<IDataReceiver<TreeOptions>> To<TTransport>(string connectionString, params KeyValuePair<string, object>[] parameters)
            where TTransport : ITransport<Stream>, new()
        {
            var transport = new TTransport();
            await transport.InitializeAsync(connectionString, parameters);
            return new Json(transport);
        }

        private Json(ITransport<Stream> transport)
        : this()
        {
            receiverTransport = transport;
        }

        public async Task ReceiveAsync(IDataProvider provider, TreeOptions query, CancellationToken token)
        {
            if (query != null && query.Transport != null)
                receiverTransport = query.Transport;
            EnsureValidForReception(query);
            using (var stream = new StreamWriter(await receiverTransport.GetStreamAsync()))
            {
                using (var writer = new JsonTextWriter(stream))
                {
                    int dataSet = -1;
                    using (var reader = await provider.ExecuteReaderAsync(token))
                    {
                        do
                        {
                            dataSet++;
                            if (dataSet > query.Matchers.Length)
                                break;
                            if (dataSet > 0)
                                throw new NotSupportedException("Currently json serialization does not support more than 1 result set");
                            var uri = query.Matchers[dataSet].rootUri;

                            for (var i = 1; i < uri.Segments.Length; i++)
                            {
                                if (uri.Segments[i] == "/")
                                {
                                    await writer.WriteStartArrayAsync();
                                }
                                else
                                {
                                    await writer.WriteStartObjectAsync();
                                    await writer.WritePropertyNameAsync(uri.Segments[i].Substring(0, uri.Segments[i].Length - 1));
                                }
                            }
                            while (reader.Read())
                            {
                                await writer.WriteStartObjectAsync();
                                for (var i = 0; i < reader.FieldCount; i++)
                                {
                                    var path = query.Matchers[dataSet].Root;
                                    var targetPath = reader.GetName(i);
                                    if (targetPath.StartsWith(path)) //if source provides paths and not just names
                                        targetPath.Substring(path.Length + 1);

                                    await Move(writer, targetPath);
                                    await writer.WriteValueAsync(reader.GetValue(i));
                                }
                                await writer.WriteEndObjectAsync();
                            }
                        }
                        while (reader.NextResult());
                    }
                    if (dataSet == -1)
                    {
                        await writer.WriteStartArrayAsync();
                        await writer.WriteEndArrayAsync();
                    }
                }
            }
        }

        private async Task Move(JsonTextWriter writer, string targetPath)
        {
            bool inObject = true;
            foreach (var segment in targetPath.Split('/'))
            {
                switch (segment)
                {

                    case "..":
                        await writer.WriteEndAsync();
                        break;
                    case "":
                        await writer.WriteStartArrayAsync();
                        inObject = false;
                        break;
                    case "text()":
                        break;
                    default:
                        if (!inObject)
                            await writer.WriteStartObjectAsync();
                        await writer.WritePropertyNameAsync(segment);
                        inObject = true;
                        break;
                }
            }
        }

        private void EnsureValidForReception(TreeOptions query)
        {
            if (query == null)
                return;
            string previous = null;
            foreach (var p in query.Matchers.Select(m => m.Root).OrderBy(p => p))
            {
                if (previous == null)
                {
                    previous = p;
                    continue;
                }
                if (p.StartsWith(previous))
                    throw new NotSupportedException("Nested roots are not supported");
            }
        }
    }

    public class JsonReceiverOptions : TreeOptions
    {
    }
}
