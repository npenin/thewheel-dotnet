using TheWheel.ETL.Contracts;
using System;
using System.Threading.Tasks;
using MailKit.Net.Smtp;
using System.Collections.Generic;
using System.Net;
using System.Linq;

namespace TheWheel.ETL.Provider.Mail
{
    public class MailTransport : ITransport<SmtpClient>
    {
        private SmtpClient client = new SmtpClient();
        Task<SmtpClient> ITransport<SmtpClient>.GetStreamAsync()
        {
            return Task.FromResult(client);
        }

        public async Task InitializeAsync(string connectionString, params KeyValuePair<string, object>[] parameters)
        {
            ICredentials credentials = null;
            var uri = new Uri(connectionString);
            if (!string.IsNullOrEmpty(uri.UserInfo))
                credentials = new NetworkCredential(uri.UserInfo.Substring(0, uri.UserInfo.IndexOf(':')), uri.UserInfo.Substring(uri.UserInfo.IndexOf(':')));
            else if (parameters != null)
                credentials = (ICredentials)parameters.FirstOrDefault(p => p.Key == "Credentials").Value;
            await client.ConnectAsync(uri.Host, uri.Port);
            if (credentials != null)
                await client.AuthenticateAsync(credentials);
        }

        public void Dispose()
        {

        }
    }
}
