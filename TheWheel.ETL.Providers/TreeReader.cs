using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Xml;
using Newtonsoft.Json;
using TheWheel.Domain;
using TheWheel.ETL.Contracts;

namespace TheWheel.ETL.Providers
{
    public abstract class TreeReader : DataReader, IConfigurable<TreeOptions, Task<IDataReader>>, IMultiDataReader
    {
        public TreeReader(string schema, string trace) : base(trace)
        {
            this.schema = schema;
        }

        public Stream BaseStream { get; private set; }

        public override int Depth => 0;

        public override int RecordsAffected => 0;

        public override int FieldCount => Current == null && matchers.Length == 1 ? matchers[0].Leaves.Count : base.FieldCount;

        public int OutputIndex { get; private set; }
        public abstract bool EndOfStream { get; }

        public async Task<IDataReader> Configure(TreeOptions options)
        {
            this.BaseStream = await options.Transport.GetStreamAsync();
            var reConfiguring = this.options == options;
            if (!reConfiguring)
            {
                this.options = options;
                supportsPaging = options.Transport is IPageable;
            }
            noMatch = true;
            ConfigureInternal(reConfiguring);

            currentItem = new Bag<string, object>();
            if (!reConfiguring)
                PrepareInternal(options);
            return this;
        }

        protected abstract void ConfigureInternal(bool reConfiguring);



        public override bool NextResult()
        {
            var t = NextResultAsync();
            t.Wait();
            return t.Result;
        }
        public async Task<bool> NextResultAsync()
        {
            if (Read())
                return true;
            if (supportsPaging)
            {
                return await ((IPageable)options.Transport).NextPage().ContinueWith<Task<bool>>(async t =>
                {
                    if (t.IsCanceled)
                        return false;
                    await Configure(this.options);
                    return await NextResultAsync();
                }).Unwrap();
            }
            return false;
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

        // public override bool IsConstant(string path)
        // {
        //     return base.IsConstant(path) || /*path.Length > 1 &&*/ path[0] >= '0' && path[0] <= '9' && base.IsConstant(path.Substring(2));
        // }

        public void PrepareInternal(TreeOptions options)
        {
            currentPath = schema;
            matchers = options.Matchers;

            for (int matcherIndex = 0; matcherIndex < options.Matchers.Length; matcherIndex++)
            {
                foreach (var path in options.Matchers[matcherIndex].Aliases.Keys)
                {
                    int offset = 0;

                    while (path[offset] == '.' && path[offset + 1] == '.')
                        offset += 3;

                    if (offset >= 3)
                    {
                        var closingPath = new Uri(matchers[matcherIndex].rootUri, path.Substring(offset % 3, offset - (offset % 3) - 1)).ToString();
                        closingPath = closingPath.Substring(0, closingPath.Length - 1);
                        List<string> keyPaths = closingPathActions.AddIfNotExists(closingPath, () => new List<string>());
                        if (!keyPaths.Contains(options.Matchers[matcherIndex].Aliases[path].Path))
                            keyPaths.Add(options.Matchers[matcherIndex].Aliases[path].Path);
                    }



                    if (path[offset] == '/')
                    {
                        absolutePaths.Add(path);
                        // matchers[i].AddPath(path);
                    }
                    //matchers[i].AddMappingIfNotExists(path, leaf.Path);
                }
            }

            keys = matchers.SelectMany(a => a.Leaves).Distinct().ToArray();

            closingPathActionsKeys = closingPathActions.Keys.ToArray();
        }

        private List<string> clearOnNextResult = new List<string>();
        // private bool parsing;
        private TreeLeaf[] keys;
        protected string currentPath;
        protected string CurrentPath => currentPath;
        // private Uri[] roots;
        private DataMatcher[] matchers;
        private Stack<int> positions = new Stack<int>();
        protected TreeOptions options;
        private IDictionary<string, object> currentItem;
        private IList<string> absolutePaths = new List<string>();
        public static readonly Regex quotesRegexParser = new Regex(@"(?<!(?<!\\)\\)'((?:[^\\']|\\[^']|\\')*)'", RegexOptions.Compiled);

        public static readonly Regex quotesRegex = new Regex(@"^'(([^\']|\[^']|\')*)'$");
        private Bag<string, List<string>> closingPathActions = new Bag<string, List<string>>();
        private string[] closingPathActionsKeys;
        private Bag<string, List<IDictionary<string, object>>> subItems = new Bag<string, List<IDictionary<string, object>>>();
        private bool supportsPaging;
        private bool noMatch;
        private readonly string schema;

        private bool StartsWithCurrentPath(string path)
        {
            return path.StartsWith(currentPath);
            //var startsWith = startsWiths.AddIfNotExists(currentPath, () => new Bag<string, bool>());
            //return startsWith.AddIfNotExists(path, () => path.StartsWith(currentPath));
        }

        public override bool Read()
        {
            if (EndOfStream)
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

            var result = DocRead(ref lastPosition, subItemPath, subItem);
            if (result)
            {
                if (supportsPaging)
                    noMatch = false;
                return true;
            }

            if (supportsPaging && !noMatch)
            {
                var t = ((IPageable)options.Transport).NextPage().ContinueWith<Task<bool>>(async t2 =>
                 {
                     if (t2.IsCanceled)
                         return false;
                     await Configure(this.options);
                     return Read();
                 }).Unwrap();
                t.Wait();
                return t.Result;
            }
            currentItem = null;
            // parsing = false;
            return false;
        }

        protected bool CloseSegment(string segment, ref Bag<string, object> subItem, ref int lastPosition, string subItemPath)
        {
            int matchingIndex = -1;
            if (segment == "/")
                currentPath = currentPath.Substring(0, currentPath.Length - segment.Length);

            for (int i = 0; i < matchers.Length; i++)
            {
                if (matchers[i].Matches(currentPath))
                {
                    matchingIndex = i;
                    break;
                }
            }


            if (closingPathActions.TryGetValue(currentPath, out var pathsToRemove))
            {
                if (matchingIndex > -1)
                    clearOnNextResult = pathsToRemove;
                else
                    foreach (var p in pathsToRemove)
                        currentItem.Remove(p);
            }
            else if (matchingIndex > -1)
                clearOnNextResult = null;
            // else
            // {
            //     foreach (var key in currentItem.Keys.ToArray())
            //     {
            //         if (!key.StartsWith(currentPath))
            //             continue;
            //         if (key.IndexOf('/', currentPath.Length) == -1)
            //             continue;
            //         currentItem.Remove(key);
            //     }
            // }

            if (segment != "/")
                currentPath = currentPath.Substring(0, currentPath.Length - segment.Length);

            if (subItemPath == currentPath)
                subItem = new Bag<string, object>();



            if (matchingIndex > -1)
            {
                OutputIndex = matchingIndex;
                Current = matchers[matchingIndex].NewRecord(currentItem);
                return true;
            }

            lastPosition = positions.Pop();
            return false;
        }

        protected void OpenSegment(string segment, ref int lastPosition, IDictionary<string, object> subItem, ref string subItemPath)
        {
            positions.Push(lastPosition + 1);
            lastPosition = -1;

            currentPath += segment;
            foreach (var key in keys)
            {
                if (key.Path == currentPath)
                {
                    var subItemList = subItems.AddIfNotExists(key.Path, () => new List<IDictionary<string, object>>());
                    subItemPath = currentPath;
                    subItemList.Add(subItem);
                }
                else if (key.Path.StartsWith(currentPath))
                {
                    switch (key.Path[currentPath.Length - 1])
                    {
                        case '/':
                            if (key.Path == currentPath + "position()")
                                currentItem.Add(key.Path, positions.Peek().ToString());
                            break;
                        case '@':
                        case '[':
                            break;
                            // default:
                            //     throw new NotSupportedException("Invalid query (" + key.Path + ") at char " + currentPath.Length);
                    }
                }
            }
        }

        protected abstract bool DocRead(ref int lastPosition, string subItemPath, Bag<string, object> subItem);

        protected void QuickPath(string subItemPath, IDictionary<string, object> subItem, string name, object value)
        {
            var path = currentPath + name;
            var constraintPath = currentPath + "[" + name;
            if (supportsPaging && path == options.TotalPath)
                ((IPageable)options.Transport).Total = Convert.ToInt32(value);
            foreach (var key in keys)
            {
                if (key.Path == path || key.Path.StartsWith(constraintPath))
                {
                    currentItem.Add(key.Path, value);
                    if (subItemPath != null && path.StartsWith(subItemPath))
                    {
                        subItem.Add(key.Path, value);
                    }
                }
            }
        }
    }
}


