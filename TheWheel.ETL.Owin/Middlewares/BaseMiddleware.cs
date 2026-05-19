using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Net.Http.Headers;
using System.Threading.Tasks;
using Newtonsoft.Json;
using TheWheel.ETL.Contracts;
using TheWheel.ETL.Providers;
using TheWheel.Domain;
using TheWheel.ETL.DacPac;
using System.Threading;
using System.Text;
using Microsoft.AspNetCore.Http;

namespace TheWheel.ETL.Owin
{
    public abstract class BaseMiddleware : IMiddleware
    {
        protected readonly Db provider;
        protected readonly IDataFormatter formatter;

        public BaseMiddleware(Db provider, IDataFormatter formatter)
        {
            this.provider = provider;
            this.formatter = formatter;
        }

        public async Task InvokeAsync(HttpContext context, RequestDelegate next)
        {
            if (context.Request.Path.HasValue)
            {
                var table = context.Request.Path;
                var indexOfSlash = -1;
                if (table.HasValue)
                    indexOfSlash = table.Value.IndexOf('/', 1);
                string tableName;
                if (indexOfSlash > -1)
                    tableName = table.Value.Substring(1, indexOfSlash - 1);
                else
                    tableName = table.Value.Substring(1);

                table.StartsWithSegments(new PathString("/" + tableName), out var id);

                var query = await GetQuery(context, tableName, id.HasValue ? id.Value.Substring(1) : null);
                if (query == null)
                    await Format(context, new SimpleDataProvider(Task.FromResult(EmptyDataReader.Empty)));
                else
                    await Format(context, await provider.QueryNewAsync(query, context.RequestAborted));
            }
            else
                await next.Invoke(context);
        }

        public abstract Task<DbQuery> GetQuery(HttpContext context, string tableName, string id);

        protected Task Format(HttpContext context, IDataProvider data)
        {
            return formatter.FormatAsync(data, context);
        }
    }
}