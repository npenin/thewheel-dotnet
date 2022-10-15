using TheWheel.ETL.Contracts;
using System;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Threading;
using MailKit.Net.Imap;
using System.Linq;
using System.Net;
using MailKit;
using System.Data;

namespace TheWheel.ETL.Provider.Mail
{
    public class MailStoreReader : DataReader, ITransport
    {
        public MailStoreReader() : base(typeof(MailStoreReader).FullName) { }

        private Queue<(MailKit.IMailFolder, int)> folders = new Queue<(IMailFolder, int)>();
        private (MailKit.IMailFolder, int) currentFolder;

        private IEnumerator<MimeKit.MimeMessage> messages;
        private ImapClient store;

        public override int Depth => currentFolder.Item2;

        public override bool IsClosed => store?.IsConnected ?? true;

        public override int RecordsAffected => currentFolder.Item1.Count;

        public override void Close()
        {
            currentFolder.Item1.Close();
        }

        public override void Dispose()
        {
            base.Dispose();
            store.Dispose();
        }


        public async Task InitializeAsync(string connectionString, CancellationToken token, params KeyValuePair<string, object>[] parameters)
        {
            Uri connectionUri = new Uri(connectionString);
            var store = this.store = new MailKit.Net.Imap.ImapClient();
            await store.ConnectAsync(connectionUri.Host, connectionUri.Port, cancellationToken: token);
            if (!string.IsNullOrEmpty(connectionUri.UserInfo))
            {
                var lastIndexOfColon = connectionUri.UserInfo.IndexOf(':');
                if (lastIndexOfColon > -1)
                    await store.AuthenticateAsync(connectionUri.UserInfo.Substring(0, lastIndexOfColon), connectionUri.UserInfo.Substring(lastIndexOfColon + 1), cancellationToken: token);
            }
            else if (parameters != null)
            {
                var cred = parameters.FirstOrDefault(p => p.Key == "Credentials");
                if (cred.Key == null)
                {
                    await store.AuthenticateAsync((ICredentials)cred.Value, cancellationToken: token);
                }
            }

            folders.Enqueue(currentFolder = (await store.GetFolderAsync(connectionUri.AbsolutePath, token), 0));
        }

        public override bool NextResult()
        {
            foreach (var subFolder in currentFolder.Item1.GetSubfolders())
                folders.Enqueue((subFolder, currentFolder.Item2 + 1));

            return folders.TryDequeue(out currentFolder);
        }

        public override bool Read()
        {
            if (currentFolder == default)
                return false;

            if (messages == null)
                messages = currentFolder.Item1.GetEnumerator();
            else
                if (!messages.MoveNext())
                return false;

            Current = DataRecord.From(messages.Current);

            return true;
        }

        public override DataTable GetSchemaTable()
        {
            throw new NotImplementedException();
        }
    }
}
