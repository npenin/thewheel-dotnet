using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using TheWheel.ETL.Contracts;

namespace TheWheel.ETL.Contracts
{
    public class TreeOptions : ITransportable<ITransport<Stream>>, IConfigurableAsync<ITransport<Stream>, TreeOptions>
    {
        public DataMatcher[] Matchers { get; private set; }

        private TreeLeaf[] paths;

        public string TotalPath;

        private string[] roots;

        public TreeOptions()
        {
        }

        public TreeOptions(ITransport<Stream> options, TreeOptions treeOptions)
        {
            this.Matchers = treeOptions.Matchers;
            this.roots = treeOptions.roots;
            this.paths = treeOptions.paths;
            this.TotalPath = treeOptions.TotalPath;
            this.Transport = options;
        }
        public TreeOptions(TreeOptions treeOptions)
        : this(treeOptions.Transport, treeOptions)
        {
        }

        public string Root
        {
            set
            {
                if (value == null)
                    Roots = null;
                else
                    Roots = new[] { value };

                BuildColumnIndices();
            }
        }

        public TreeOptions AddMatch(string root, params TreeLeaf[] paths)
        {
            string[] roots;
            if (this.roots == null)
                roots = new string[1];
            else
            {
                roots = new string[this.roots.Length + 1];
                Array.Copy(this.roots, roots, this.roots.Length);
            }
            if (root[root.Length - 1] == '/')
                root = root.Substring(0, root.Length - 1);
            roots[roots.Length - 1] = root;
            this.roots = roots;
            TreeLeaf[] newPaths;
            int pathOffset = 0;
            if (this.paths == null)
                newPaths = new TreeLeaf[paths.Length];
            else
            {
                newPaths = new TreeLeaf[this.paths.Length + paths.Length];
                Array.Copy(this.paths, newPaths, this.paths.Length);
                pathOffset = this.paths.Length;
            }
            var pathPrefix = (roots.Length - 1).ToString() + '/';
            for (int i = pathOffset; i < newPaths.Length; i++)
                newPaths[i] = new TreeLeaf(pathPrefix + paths[i - pathOffset].Path, paths[i - pathOffset].TypeCode);
            Paths = newPaths;
            return this;
        }

        private void BuildColumnIndices()
        {
            if (roots == null || paths == null)
                return;
            if (Roots.Length > 10)
                throw new ArgumentOutOfRangeException("Json does not support more than 10 roots");
            Matchers = new DataMatcher[Roots.Length];
            for (int i = 0; i < Matchers.Length; i++)
            {
                Matchers[i] = new DataMatcher(i, Roots[i]);
            }
            for (int i = 0; i < Paths.Length; i++)
            {
                var p = Paths[i];
                if (p.Path[0] >= '0' && p.Path[0] <= '9')
                {
                    var index = p.Path[0] - '0';
                    // p.Path = p.Path.Substring(2);
                    Matchers[index].AddPath(p.Path.Substring(2), p.TypeCode);
                }
                else if (roots.Length == 1)
                    Matchers[0].AddPath(p.Path, p.TypeCode);
                else
                    Array.ForEach(Matchers, m => m.AddPath(p.Path, p.TypeCode));
            }
        }

        internal void Merge(TreeOptions option)
        {
            if (this.Roots != null && option.Roots != null)
                throw new InvalidOperationException("Roots were already specified");
            if (this.Paths != null && option.Paths != null)
                throw new InvalidOperationException("Paths were already specified");
            if (this.Roots == null)
                Roots = option.Roots;
            if (this.paths == null)
                Paths = option.paths;
        }

        public Task<TreeOptions> Configure(ITransport<Stream> options, CancellationToken token)
        {
            return Task.FromResult(new TreeOptions(options, this));
        }

        public TreeLeaf[] Paths
        {
            get => paths; set
            {
                paths = value;
                BuildColumnIndices();
            }
        }

        public string[] Roots { private get => roots; set => roots = value; }

        public ITransport<Stream> Transport
        {
            get; set;
        }
    }
}