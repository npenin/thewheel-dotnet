using ODP = Oracle.ManagedDataAccess;

namespace TheWheel.ETL.Providers
{
    public class Oracle : Db
    {
        public Oracle()
            : base(ODP.Client.OracleClientFactory.Instance)
        {

        }
    }
}
