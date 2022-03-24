using System.Threading.Tasks;
using System.Collections.Generic;
using System.DirectoryServices.Protocols;
using System.Linq;
using System.Net;

namespace TheWheel.ETL.Provider.Ldap
{
    public class LdapTransport : Contracts.ITransport<LdapConnection>
    {
        private LdapConnection ldapConnection;

        public Task InitializeAsync(string connectionString, params KeyValuePair<string, object>[] parameters)
        {
            var id = new LdapDirectoryIdentifier(connectionString);
            if (parameters != null)
            {
                var creds = parameters.FirstOrDefault(p => p.Key == "Credentials").Value as NetworkCredential;
                if (creds != null)
                {
                    this.ldapConnection = new LdapConnection(id, creds);
                    return Task.CompletedTask;
                }
            }
            this.ldapConnection = new LdapConnection(id);
            return Task.CompletedTask;
        }

        public Task<LdapConnection> GetStreamAsync()
        {
            return Task.FromResult(this.ldapConnection);
        }

        public void Dispose()
        {
            if (this.ldapConnection != null)
                this.ldapConnection.Dispose();
        }
    }
}