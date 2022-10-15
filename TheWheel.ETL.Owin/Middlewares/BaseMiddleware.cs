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
    public abstract class BaseMiddleware<TQuery> : IMiddleware
    {
        public string[] IngoredSchema { get; private set; }

        private static ReaderWriterLockSlim lockObject = new ReaderWriterLockSlim();

        protected readonly IAsyncNewQueryable<TQuery> provider;

        public static Dictionary<string, Func<IDataProvider, HttpContext, Task>> Formatters { get; } = new Dictionary<string, Func<IDataProvider, HttpContext, Task>>();

        public static void AddJsonFormatter()
        {
            RegisterReveiverFormatter<Json, TreeOptions>("application/json", () =>
            {
                Console.WriteLine("building options for application/json");
                return new TreeOptions().AddMatch("json:///");
            });
            RegisterReveiverFormatter<Json, TreeOptions>("text/json", () =>
            {
                Console.WriteLine("building options for text/json");
                return new TreeOptions().AddMatch("json:///");
            });
        }

        public static void AddCsvFormatter()
        {
            RegisterReveiverFormatter<Csv, CsvReceiverOptions>("text/csv");
        }

        public static void RegisterReveiverFormatter<T, TOptions>(string mediaType)
        where T : IDataReceiver<TOptions>, new()
        where TOptions : IConfigurableAsync<ITransport<Stream>, TOptions>, new()
        {
            RegisterReveiverFormatter<T, TOptions>(mediaType, () => new TOptions());
        }

        public static void RegisterReveiverFormatter<T, TOptions>(string mediaType, Func<TOptions> optionsFactory)
        where T : IDataReceiver<TOptions>, new()
        where TOptions : IConfigurableAsync<ITransport<Stream>, TOptions>
        {
            Formatters.Add(mediaType, async (provider, context) =>
             {
                 var receiver = new T();
                 var options = optionsFactory();
                 context.Response.ContentType = mediaType;
                 await options.Configure(new StreamTransport().Configure(context.Response.Body), context.RequestAborted);
                 await receiver.ReceiveAsync(provider, options, new System.Threading.CancellationTokenSource().Token);
             });
        }


        public BaseMiddleware(IAsyncNewQueryable<TQuery> provider)
        {
            this.provider = provider;
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

        public abstract Task<TQuery> GetQuery(HttpContext context, string tableName, string id);

        protected Task Format(HttpContext context, IDataProvider data)
        {
            var accepts = context.Request.Headers.GetCommaSeparatedValues("Accept").Select(h => MediaTypeWithQualityHeaderValue.TryParse(h, out var accept) ? accept : null).OrderByDescending(h => h.Quality);

            foreach (var accept in accepts)
            {
                if (Formatters.TryGetValue(accept.MediaType, out var formatter))
                    return formatter(data, context);
            }

            var json = new Json();
            return json.ReceiveAsync(data, new TreeOptions() { Transport = new StreamTransport().Configure(context.Response.Body) }.AddMatch("json:///"), context.RequestAborted);
        }
    }
}