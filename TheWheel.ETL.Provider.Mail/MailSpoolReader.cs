using TheWheel.ETL.Contracts;
using System;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Threading;
using MailKit.Net.Pop3;
using System.Linq;
using System.Net;
using MailKit;
using System.Data;

namespace TheWheel.ETL.Provider.Mail
{
    public class MailSpoolReader : DataReader, ITransport, IPageable
    {
        public MailSpoolReader() : base(typeof(MailStoreReader).FullName) { }

        private IEnumerator<MimeKit.MimeMessage> messages;
        private Pop3Client store;

        private int startIndex;
        private int pageSize = 50;

        public override int Depth => 0;

        public override bool IsClosed => store?.IsConnected ?? true;

        public override int RecordsAffected => Total;

        public int Total { get; set; }

        public override void Close()
        {
        }

        public override void Dispose()
        {
            base.Dispose();
            store.Dispose();
        }


        public async Task InitializeAsync(string connectionString, CancellationToken token, params KeyValuePair<string, object>[] parameters)
        {
            Uri connectionUri = new Uri(connectionString);
            var store = this.store = new Pop3Client();
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

            Total = store.Count;

            startIndex = 0;
            if (parameters != null)
            {
                var pageSize = parameters.FirstOrDefault(p => p.Key == "Batch");
                if (pageSize.Key != default)
                    this.pageSize = Convert.ToInt32(pageSize.Value);
            }

            await NextPage(token);
        }

        public override bool NextResult()
        {
            NextPage(CancellationToken.None).Wait();
            return Read();
        }

        public override bool Read()
        {
            if (!messages.MoveNext())
                return false;

            Current = DataRecord.From(messages.Current);

            return true;
        }

        public override DataTable GetSchemaTable()
        {
            throw new NotImplementedException();
        }

        public async Task NextPage(CancellationToken token)
        {
            if (pageSize < Total)
            {
                messages = (await store.GetMessagesAsync(startIndex, pageSize, token)).GetEnumerator();
                startIndex += pageSize;
            }
            else
                messages = null;
        }
    }
}
