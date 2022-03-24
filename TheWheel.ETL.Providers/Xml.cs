using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Xml;
using Newtonsoft.Json;
using TheWheel.ETL.Contracts;

namespace TheWheel.ETL.Providers
{
    public class Xml : DataReader, IConfigurable<TreeOptions, Task<IDataReader>>, IMultiDataReader
    {
        public Xml() : base("TheWheel.ETL.Providers.Xml")
        {

        }


        public static async Task<DataProvider<Xml, TreeOptions, ITransport<Stream>>> From<TTransport>(string connectionString, params KeyValuePair<string, object>[] parameters)
            where TTransport : ITransport<Stream>, new()
        {
            var provider = new DataProvider<Xml, TreeOptions, ITransport<Stream>>();
            await provider.InitializeAsync(new TTransport());
            await provider.Transport.InitializeAsync(connectionString, parameters);
            return provider;
        }
        public Stream BaseStream { get; private set; }

        public override int Depth => 0;

        public override bool IsClosed => reader.ReadState != ReadState.Closed;

        public override int RecordsAffected => 0;

        public int OutputIndex { get; private set; }

        private XmlReader reader;

        public async Task<IDataReader> Configure(TreeOptions options)
        {
            this.BaseStream = await options.Transport.GetStreamAsync();
            reader = XmlReader.Create(this.BaseStream, new XmlReaderSettings
            {
                DtdProcessing = DtdProcessing.Ignore,
                CloseInput = true,
                IgnoreComments = true,
                IgnoreProcessingInstructions = true,
            });
            this.options = options;

            currentItem = new Bag<string, object>();
            PrepareInternal(options);
            return this;
        }

        public override void Close()
        {
            this.BaseStream.Close();
        }

        public override void Dispose()
        {
            base.Dispose();
            this.BaseStream.Dispose();
        }

        public override DataTable GetSchemaTable()
        {
            throw new NotImplementedException();
        }

        public override bool NextResult()
        {
            return Read();
        }

        // public override bool IsConstant(string path)
        // {
        //     return base.IsConstant(path) || /*path.Length > 1 &&*/ path[0] >= '0' && path[0] <= '9' && base.IsConstant(path.Substring(2));
        // }

        public void PrepareInternal(TreeOptions options)
        {
            keys = options.Paths;
            currentPath = "xml://";
            matchers = options.Matchers;

            foreach (var key in keys.Select((k, i) => new { Key = k, Index = i }))
            {
                for (var i = 0; i < matchers.Length; i++)
                {
                    if (key.Key.Path[0] - '0' != i && key.Key.Path[0] >= '0' && key.Key.Path[0] <= '9')
                        continue;

                    string path;
                    int offset;
                    if (key.Key.Path[0] - '0' == i)
                    {
                        offset = 2;
                    }
                    else
                    {
                        offset = 0;
                    }

                    path = key.Key.Path.Substring(offset);
                    // if (quotesRegex.IsMatch(path))
                    // {
                    //     MatchCollection matches = quotesRegexParser.Matches(path);
                    //     if (matches.Count > 1)
                    //     {
                    //         var evaluations = new List<string>();
                    //         for (var m = 1; m < matches.Count; m++)
                    //         {
                    //             var subOffset = 0;
                    //             var evaluation = path.Substring(matches[m - 1].Index + matches[m - 1].Length, matches[m].Index - matches[m - 1].Index - matches[m - 1].Length);
                    //             var subPath = new Uri(roots[i], evaluation.Substring(subOffset)).ToString();

                    //             while (evaluation[0 + subOffset] == '.' && evaluation[1 + subOffset] == '.')
                    //             {
                    //                 subOffset += 3;
                    //             }

                    //             if (subOffset >= 3)
                    //             {
                    //                 var closingPath = new Uri(roots[i], key.Key.Substring(subOffset % 3, subOffset - (subOffset % 3) - 1)).ToString();
                    //                 closingPath = closingPath.Substring(0, closingPath.Length - 1);
                    //                 List<string> keyPaths = closingPathActions.AddIfNotExists(closingPath, () => new List<string>());
                    //                 if (!keyPaths.Contains(subPath))
                    //                     keyPaths.Add(subPath);
                    //             }

                    //             if (subPath == "json://" + evaluation)
                    //                 absolutePaths.Add(subPath);
                    //             aliases[i].AddIfNotExists(evaluation, () => subPath);
                    //             aliases[i].AddIfNotExists(key.Key[0] + "/" + evaluation, () => subPath);
                    //         }
                    //     }
                    //     continue;
                    // }


                    key.Key.Path = new Uri(matchers[i].rootUri, path).ToString();


                    while (key.Key.Path[0 + offset] == '.' && key.Key.Path[1 + offset] == '.')
                    {
                        offset += 3;
                    }

                    if (offset >= 3)
                    {
                        var closingPath = new Uri(matchers[i].rootUri, key.Key.Path.Substring(offset % 3, offset - (offset % 3) - 1)).ToString();
                        closingPath = closingPath.Substring(0, closingPath.Length - 1);
                        List<string> keyPaths = closingPathActions.AddIfNotExists(closingPath, () => new List<string>());
                        if (!keyPaths.Contains(path))
                            keyPaths.Add(path);
                    }

                    if (path == "xml://" + key.Key)
                    {
                        absolutePaths.Add(path);
                        // matchers[i].AddPath(path);
                    }
                    matchers[i].AddMappingIfNotExists(path, key.Key);
                }
            }
            keys = matchers.SelectMany(a => a.Leaves).Distinct().ToArray();

            closingPathActionsKeys = closingPathActions.Keys.ToArray();
        }

        private List<string> clearOnNextResult = new List<string>();
        // private bool parsing;
        private TreeLeaf[] keys;
        private string currentPath;
        // private Uri[] roots;
        private DataMatcher[] matchers;
        private Stack<int> positions = new Stack<int>();
        private TreeOptions options;
        private IDictionary<string, object> currentItem;
        private IList<string> absolutePaths = new List<string>();
        public static readonly Regex quotesRegexParser = new Regex(@"(?<!(?<!\\)\\)'((?:[^\\']|\\[^']|\\')*)'", RegexOptions.Compiled);

        public static readonly Regex quotesRegex = new Regex(@"^'(([^\']|\[^']|\')*)'$");
        private Bag<string, List<string>> closingPathActions = new Bag<string, List<string>>();
        private string[] closingPathActionsKeys;
        private Bag<string, List<Bag<string, object>>> subItems = new Bag<string, List<Bag<string, object>>>();

        private bool StartsWithCurrentPath(string path)
        {
            return path.StartsWith(currentPath);
            //var startsWith = startsWiths.AddIfNotExists(currentPath, () => new Bag<string, bool>());
            //return startsWith.AddIfNotExists(path, () => path.StartsWith(currentPath));
        }

        public override bool Read()
        {
            if (reader.EOF)
                return false;
            // parsing = true;
            string subItemPath = null;
            Bag<string, object> subItem = new Bag<string, object>();

            int lastPosition = -1;
            if (positions.Count > 0)
                lastPosition = positions.Peek();

            if (clearOnNextResult != null)
                foreach (var path in clearOnNextResult)
                {
                    currentItem.Remove(path);
                }


            var shouldRemove = closingPathActionsKeys != null && closingPathActionsKeys.AsParallel().Any(cpa => cpa.Length > currentPath.Length && cpa.IndexOf('/', currentPath.Length) == -1);

            foreach (var key in currentItem.Keys.Where(k => !absolutePaths.Contains(k) && StartsWithCurrentPath(k)).ToArray())
            {
                if (shouldRemove)
                    currentItem.Remove(key);

                if (!closingPathActions.Any(cpa => cpa.Value.Any(p => p == key)))
                    currentItem.Remove(key);
            }
            foreach (var key in subItems.Keys.Where(k => !absolutePaths.Contains(k) && StartsWithCurrentPath(k)).ToArray())
            {
                subItems.Remove(key);
            }

            string constraintPath;
            List<string> pathsToRemove;
            int matchingIndex = -1;

            while (DocRead())
            {
                string path;
                switch (reader.NodeType)
                {
                    case XmlNodeType.Attribute:
                        path = currentPath + "/@" + reader.LocalName;
                        constraintPath = currentPath + "[@" + reader.LocalName;
                        foreach (var key in keys)
                        {
                            if (key.Path == path || key.Path.StartsWith(constraintPath))
                            {
                                currentItem.Add(key.Path, reader.Value);
                                if (subItemPath != null && path.StartsWith(subItemPath))
                                {
                                    subItem.Add(key.Path, reader.Value);
                                }
                            }
                        }
                        break;
                    case XmlNodeType.CDATA:
                    case XmlNodeType.Text:
                        path = currentPath + "/text()";
                        constraintPath = currentPath + "[";
                        foreach (var key in keys)
                        {
                            var matches = key.Path == path;
                            if (key.Path.StartsWith(constraintPath))
                            {
                                var constraint = key.Path.Substring(constraintPath.Length).Trim();
                                // var method = this.isMethod.AddIfNotExists(key, () => methodRegex.Match(key));
                                // if (method.Success)
                                // {
                                //     constraint = constraint.Substring(method.Index + method.Length - 1);

                                //     var arg = methodParamsRegex.Match(constraint);
                                //     List<string> args = new List<string>();
                                //     foreach (Capture param in arg.Groups[1].Captures)
                                //     {
                                //         string paramValue = param.Value;
                                //         var quotedArg = quotesRegex.Match(paramValue);
                                //         if (quotedArg.Success)
                                //             args.Add(quotedArg.Groups[1].Value.Replace("\\'", "'"));
                                //         else if (paramValue == "text()" || paramValue == ".")
                                //             args.Add(doc.Value);
                                //         else
                                //             throw new ArgumentException(paramValue);
                                //     }

                                //     switch (method.Groups[1].Value)
                                //     {
                                //         case "contains":
                                //             if (args.Count != 2)
                                //                 throw new InvalidOperationException("2 argument is exepected, but " + args.Count + "were found");
                                //             matches = args[0].Contains(args[1]);
                                //             break;
                                //         default:
                                //             throw new NotSupportedException(method.Groups[1].Value + " is not supported");
                                //     }
                                // }
                            }
                            if (matches)
                            {
                                currentItem.Add(key.Path, reader.Value);// on purpose to have multiple values for the same key

                                if (subItemPath != null && path.StartsWith(subItemPath))
                                {
                                    subItem.Add(path, reader.Value);
                                }
                            }
                        }
                        break;
                    case XmlNodeType.Comment:
                        break;
                    case XmlNodeType.Document:
                        break;
                    case XmlNodeType.DocumentFragment:
                        break;
                    case XmlNodeType.DocumentType:
                        break;
                    case XmlNodeType.Element:
                        positions.Push(lastPosition + 1);
                        lastPosition = -1;

                        currentPath += '/' + reader.LocalName;
                        foreach (var key in keys)
                        {
                            if (key.Path == currentPath)
                            {
                                var subItemList = subItems.AddIfNotExists(key.Path, () => new List<Bag<string, object>>());
                                subItemPath = currentPath;
                                subItemList.Add(subItem);
                            }
                            else if (key.Path.StartsWith(currentPath))
                            {
                                switch (key.Path[currentPath.Length])
                                {
                                    case '/':
                                        if (key.Path == currentPath + "/position()")
                                            currentItem.Add(key.Path, positions.Peek().ToString());
                                        break;
                                    case '@':
                                    case '[':
                                        break;
                                    default:
                                        throw new NotSupportedException("Invalid query (" + key + ") at char " + currentPath.Length);
                                }
                            }
                        }

                        if (reader.IsEmptyElement)
                        {
                            for (int i = 0; i < matchers.Length; i++)
                            {
                                if (matchers[i].Matches(currentPath))
                                {
                                    matchingIndex = i;
                                    break;
                                }

                            }

                            if (closingPathActions.TryGetValue(currentPath, out pathsToRemove))
                            {
                                if (matchingIndex > -1)
                                    throw new NotSupportedException();

                                foreach (var p in pathsToRemove)
                                    currentItem.Remove(p);
                            }

                            if (subItemPath == currentPath)
                                subItem = new Bag<string, object>();
                            currentPath = currentPath.Substring(0, currentPath.Length - reader.LocalName.Length - 1);

                            if (matchingIndex > -1)
                            {
                                OutputIndex = matchingIndex;
                                Current = matchers[OutputIndex].NewRecord(currentItem);
                                return true;
                            }

                            lastPosition = positions.Pop();
                        }
                        break;
                    case XmlNodeType.EndElement:

                        for (int i = 0; i < matchers.Length; i++)
                        {
                            if (matchers[i].Matches(currentPath))
                            {
                                matchingIndex = i;
                                break;
                            }

                        }

                        if (closingPathActions.TryGetValue(currentPath, out pathsToRemove))
                        {
                            if (matchingIndex > -1)
                                clearOnNextResult = pathsToRemove;
                            else
                                foreach (var p in pathsToRemove)
                                    currentItem.Remove(p);
                        }
                        else if (matchingIndex > -1)
                            clearOnNextResult = null;


                        if (subItemPath == currentPath)
                            subItem = new Bag<string, object>();
                        currentPath = currentPath.Substring(0, currentPath.Length - reader.LocalName.Length - 1);


                        if (matchingIndex > -1)
                        {
                            OutputIndex = matchingIndex;
                            Current = matchers[matchingIndex].NewRecord(currentItem);
                            return true;
                        }

                        lastPosition = positions.Pop();
                        break;
                    case XmlNodeType.EndEntity:
                        break;
                    case XmlNodeType.Entity:
                        break;
                    case XmlNodeType.EntityReference:
                        break;
                    case XmlNodeType.None:
                        break;
                    case XmlNodeType.Notation:
                        break;
                    case XmlNodeType.ProcessingInstruction:
                        break;
                    case XmlNodeType.SignificantWhitespace:
                        break;
                    case XmlNodeType.Whitespace:
                        break;
                    case XmlNodeType.XmlDeclaration:
                        break;
                    default:
                        break;
                }
            }
            currentItem = null;
            // parsing = false;
            return false;
        }

        private bool DocRead()
        {
            switch (reader.NodeType)
            {
                case XmlNodeType.Attribute:
                    return reader.MoveToNextAttribute() || reader.Read();
                case XmlNodeType.Element:
                    return reader.MoveToFirstAttribute() || reader.Read();
                default:
                    return reader.Read();
            }
        }
    }
}


