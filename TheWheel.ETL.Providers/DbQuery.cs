using System;
using System.Collections.Generic;
using System.Data;

namespace TheWheel.ETL.Providers
{
    public class DbQuery
    {
        public DbQuery(DbQuery query)
        {
            this.query = query.query;
            this.parameters = query.parameters;
            this.Timeout = query.Timeout;
        }

        public DbQuery(DbQuery query, string newQueryString)
        : this(query)
        {
            this.query = newQueryString;
        }

        private readonly string query;
        private readonly KeyValuePair<string, object>[] parameters;

        public int? Timeout;
        public string Text => query;

        public IDbTransaction Transaction;

        public KeyValuePair<string, object>[] Parameters => parameters;

        public DbQuery(string query, params KeyValuePair<string, object>[] parameters)
        {
            this.query = query;
            this.parameters = parameters;
        }

        public static implicit operator DbQuery(string query)
        {
            return new DbQuery(query);
        }
    }
}