using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using TheWheel.ETL.Contracts;
using TheWheel.Domain;

namespace TheWheel.ETL.Providers
{
    public abstract class File : ITransport<Stream>
    {
        protected string path;
        protected string archivePath;

        public void Dispose()
        {
            if (archivePath != null)
                System.IO.File.Move(path, archivePath);
        }

        public virtual Task<Stream> GetStreamAsync(CancellationToken token)
        {
            return Task.FromResult<Stream>(System.IO.File.Open(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read));
        }

        public Task InitializeAsync(string connectionString, CancellationToken token, params KeyValuePair<string, object>[] parameters)
        {
            path = connectionString;
            return Task.CompletedTask;
        }

        public Task Configure(FileConfiguration configuration)
        {
            if (string.IsNullOrEmpty(configuration.ArchivePath))
            {
                var fi = new FileInfo(configuration.ArchivePath);
                if (fi.Exists)
                    if (fi.Attributes.HasFlag(FileAttributes.Directory))
#if NET5_0
                        archivePath = Path.Join(configuration.ArchivePath, Path.GetFileNameWithoutExtension(path) + DateTime.UtcNow.ToString("yyyyMMddHHmmss") + Path.GetExtension(path));
#else
                        archivePath = Path.Combine(configuration.ArchivePath, Path.GetFileNameWithoutExtension(path) + DateTime.UtcNow.ToString("yyyyMMddHHmmss") + Path.GetExtension(path));
#endif
                    else
                        archivePath = configuration.ArchivePath;
            }
            return Task.CompletedTask;
        }
    }

    public class FileConfiguration
    {
        public string ArchivePath { get; internal set; }
    }

    public class FileRead : File, IPageable
    {
        private static Bag<string, string> fileMapping = new Bag<string, string>();
        private System.Text.RegularExpressions.Regex zipFileMatcher = new System.Text.RegularExpressions.Regex(@"\.zip[/\\]");
        private Queue<string> fileQueue = new Queue<string>();

        public int Total { get; set; }

        public override async Task<Stream> GetStreamAsync(CancellationToken token)
        {
            var path = this.path;
            var unzip = zipFileMatcher.Match(path);
            if (unzip.Success)
            {
                var zippedFile = path.Substring(unzip.Index + 5);
                path = path.Substring(0, unzip.Index + 4);
                if (fileMapping.TryGetValue(path, out var file))
                {
                    if (System.IO.File.Exists(file))
                        path = file;
                    else
                        fileMapping.Remove(path);
                }

                var zipFile = path;
                // trace.TraceInformation("Opening zip file " + path);
                using (var zip = System.IO.Compression.ZipFile.OpenRead(path))
                {
                    if (zippedFile.Contains("*"))
                    {
                        var regex = new Regex("^" + Regex.Escape(zippedFile).Replace("\\?", ".").Replace("\\*", ".*") + "$");
                        foreach (var entry in zip.Entries)
                        {
                            if (regex.IsMatch(entry.Name))
                            {
                                var tmpFile = Path.GetTempFileName();
                                using (var tmpStream = System.IO.File.OpenWrite(tmpFile))
                                    entry.Open().CopyTo(tmpStream);
                                fileMapping.Add(zipFile + "/" + entry.Name, tmpFile);
                                fileQueue.Enqueue(zipFile + "/" + entry.Name);
                            }
                        }
                        zippedFile = null;
                        // trace.TraceInformation("file list:" + string.Join(", ", fileQueue.ToArray()));
                        path = fileMapping[fileQueue.Dequeue()];
                    }
                    else
                    {
                        var entry = zip.GetEntry(zippedFile);

                        var tmpFile = Path.GetTempFileName();
                        using (var tmpStream = System.IO.File.OpenWrite(tmpFile))
                        {
                            if (entry == null)
                            {
                                Console.WriteLine(zippedFile + " was not found in " + zipFile);
                            }
                            var entryStream = entry.Open();

#if NET5_0_OR_GREATER
                            await entryStream.CopyToAsync(tmpStream, token);
#else
                            entryStream.CopyTo(tmpStream);
#endif
                        }

                        path = tmpFile;
                    }
                }
            }

            return System.IO.File.Open(path, FileMode.Open, FileAccess.Read, FileShare.Read);
        }

        public Task NextPage(CancellationToken token)
        {
#if NET5_0
            if (fileQueue.TryDequeue(out path))
                return Task.CompletedTask;
#else
            if (fileQueue.Count > 0)
            {
                path = fileQueue.Dequeue();
                return Task.CompletedTask;
            }
#endif
            return Task.FromCanceled(new System.Threading.CancellationToken(true));
        }
    }

    public class FileWrite : File
    {
        public override Task<Stream> GetStreamAsync(CancellationToken token)
        {
            return Task.FromResult<Stream>(System.IO.File.Open(path, FileMode.Create, FileAccess.Write, FileShare.Read));
        }
    }

}