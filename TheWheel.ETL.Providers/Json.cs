using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using TheWheel.Domain;
using TheWheel.ETL.Contracts;

namespace TheWheel.ETL.Providers
{
    public partial class Json : TreeReader
    {
        public Json()
        : base("json://", "TheWheel.ETL.Providers.Json")
        {

        }

        public static Task<DataProvider<Json, TreeOptions, ITransport<Stream>>> From<TTransport>(string connectionString, CancellationToken token, params KeyValuePair<string, object>[] parameters)
            where TTransport : ITransport<Stream>, new()
        {
            return From(new TTransport(), token, connectionString, parameters);
        }
        public static async Task<DataProvider<Json, TreeOptions, ITransport<Stream>>> From<TTransport>(TTransport transport, CancellationToken token, string connectionString, params KeyValuePair<string, object>[] parameters)
            where TTransport : ITransport<Stream>
        {
            var provider = new DataProvider<Json, TreeOptions, ITransport<Stream>>();
            await transport.InitializeAsync(connectionString, token, parameters);
            provider.Initialize(transport);
            return provider;
        }

        private JsonTextReader reader;

        protected override void ConfigureInternal(bool reConfiguring)
        {
            reader = new JsonTextReader(new StreamReader(BaseStream));
        }

        public override bool EndOfStream => !BaseStream.CanRead;

        public override bool IsClosed => reader.TokenType != JsonToken.None;

        private bool StartsWithCurrentPath(string path)
        {
            return path.StartsWith(currentPath);
            //var startsWith = startsWiths.AddIfNotExists(currentPath, () => new Bag<string, bool>());
            //return startsWith.AddIfNotExists(path, () => path.StartsWith(currentPath));
        }

        private string closeSegment = null;

        protected override bool DocRead(ref int lastPosition, string subItemPath, Bag<string, object> subItem)
        {
            if (closeSegment != null)
            {
                if (CloseSegment(closeSegment, ref subItem, ref lastPosition, subItemPath))
                {
                    closeSegment = null;
                    return true;
                }
                closeSegment = null;
            }
            while (reader.Read())
            {
                switch (reader.TokenType)
                {
                    case JsonToken.PropertyName:
                        OpenSegment(reader.Value.ToString(), ref lastPosition, subItem, ref subItemPath);
                        // if (currentPath[currentPath.Length - 1] != '/')
                        //     currentPath = currentPath.Substring(0, currentPath.LastIndexOf('/') + 1);
                        // path = currentPath + reader.Value;

                        // constraintPath = currentPath + "[@" + reader.Value;

                        break;
                    case JsonToken.Date:
                    case JsonToken.Integer:
                    case JsonToken.Float:
                    case JsonToken.Boolean:
                    case JsonToken.String:
                    case JsonToken.Null:
                        QuickPath(subItemPath, subItem, "/text()", reader.Value);
                        if (currentPath[currentPath.Length - 1] != '/')
                            CloseSegment(currentPath.Substring(currentPath.LastIndexOf('/') + 1), ref subItem, ref lastPosition, subItemPath);
                        break;
                    case JsonToken.Comment:
                        break;
                    case JsonToken.StartObject:
                    // OpenSegment("/", ref lastPosition, subItem, ref subItemPath);
                    // break;
                    case JsonToken.StartArray:
                        OpenSegment("/", ref lastPosition, subItem, ref subItemPath);
                        break;
                    case JsonToken.EndObject:
                    // if (currentPath[currentPath.Length - 1] == '/')
                    // {
                    //     if (CloseSegment(currentPath.Substring(currentPath.LastIndexOf('/') + 1), ref subItem, ref lastPosition, subItemPath))
                    //         return true;
                    // }
                    // else if (CloseSegment(currentPath.Substring(currentPath.LastIndexOf('/')), ref subItem, ref lastPosition, subItemPath))
                    //     return true;
                    // break;
                    case JsonToken.EndArray:
                        if (CloseSegment(currentPath.Substring(currentPath.LastIndexOf('/')), ref subItem, ref lastPosition, subItemPath))
                        {
                            if (currentPath[currentPath.Length - 1] != '/')
                                closeSegment = currentPath.Substring(currentPath.LastIndexOf('/') + 1);
                            return true;
                        }

                        if (currentPath[currentPath.Length - 1] != '/' && CloseSegment(currentPath.Substring(currentPath.LastIndexOf('/') + 1), ref subItem, ref lastPosition, subItemPath))
                            return true;

                        break;
                    default:
                        break;
                }
            }
            return false;
        }

        public static string Extract(string root, IDataRecord record)
        {
            var keys = Enumerable.Range(0, record.FieldCount).Select(i => new KeyValuePair<string, int>(record.GetName(i), i)).Where(k => k.Key.StartsWith(root) && k.Key != root + "/*").ToArray();
            var aliases = new Bag<string, int>(keys.Select(k => new KeyValuePair<string, int>(k.Key.Substring(root.Length), k.Value)));
            var structure = aliases.Keys.GroupBy(k => k, (key, list) => new { Key = key, Count = list.Count() }).OrderBy(k => k.Key).ToArray();

            var currentPath = string.Empty;

            using (var ms = new StringWriter())
            {
                using (var writer = new JsonTextWriter(ms))
                {
                    int indexOfSlash;
                    foreach (var path in structure)
                    {
                        var isText = false;
                        var commonOffset = 0;
                        for (commonOffset = 0; commonOffset < path.Key.Length && commonOffset < currentPath.Length; commonOffset++)
                        {
                            if (path.Key[commonOffset] != currentPath[commonOffset])
                                break;
                        }
                        for (var offset = currentPath.Length; offset > commonOffset; offset = currentPath.LastIndexOf('/', offset - 1))
                        {
                            if (offset > 0 && currentPath[offset - 1] == '/')
                            {
                                writer.WriteEndArray();
                                if (offset == currentPath.Length)
                                    offset--;
                            }
                            else if (offset < currentPath.Length)
                                writer.WriteEndObject();
                        }
                        if (commonOffset > 0)
                            if (commonOffset == currentPath.Length)
                                commonOffset = currentPath.LastIndexOf('/');
                            else
                                commonOffset = currentPath.LastIndexOf('/', commonOffset);
                        if (commonOffset + 1 < currentPath.Length)
                        {
                            currentPath = currentPath.Substring(0, ++commonOffset);
                        }
                        for (var offset = commonOffset; offset < path.Key.Length;)
                        {

                            while (path.Key[offset] == '/' && (offset == 0 || path.Key[offset + 1] == '/'))
                            {
                                writer.WriteStartArray();
                                currentPath += '/';
                                offset++;
                            }

                            while (writer.WriteState != WriteState.Object && path.Key[offset] == '/' && path.Key[offset + 1] != '/')
                            {
                                isText = path.Key.Substring(offset) == "/text()";
                                if (isText)
                                    break;
                                writer.WriteStartObject();
                                currentPath += '/';
                                offset++;
                            }

                            indexOfSlash = path.Key.IndexOf('/', offset);
                            if (indexOfSlash > offset)
                            {
                                var key = path.Key.Substring(offset, indexOfSlash - offset);
                                writer.WritePropertyName(key);
                                offset = indexOfSlash;
                                currentPath += key;
                            }

                            if (isText)
                            {
                                if (path.Count == 1)
                                    writer.WriteValue(record.GetValue(aliases[path.Key]));
                                else
                                {
                                    var values = (IList<string>)record.GetValue(aliases[path.Key]);
                                    foreach (var value in values)
                                        writer.WriteValue(value);
                                }
                                isText = false;
                                break;
                            }
                        }
                    }
                }
                return ms.ToString();
            }

        }
    }
}
