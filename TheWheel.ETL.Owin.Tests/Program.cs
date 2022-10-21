using System.Reflection;

var builder = WebApplication.CreateBuilder(args);
builder.Configuration.AddUserSecrets(Assembly.GetExecutingAssembly(), true);

builder.Services.AddOptions<TheWheel.ETL.Owin.PolicyConfiguration>().BindConfiguration("TheWheel:ETL");
builder.Services.AddScoped<TheWheel.ETL.Owin.IPolicyProvider>((services) => services.GetService<Microsoft.Extensions.Options.IOptions<TheWheel.ETL.Owin.PolicyConfiguration>>().Value);
builder.Services.AddScoped<TheWheel.ETL.Contracts.IAsyncNewQueryable<TheWheel.ETL.Providers.DbQuery>>((services) =>
{
    var t = TheWheel.ETL.Providers.Sql.From("Server=tcp:sqldnadb.database.windows.net,1433;Initial Catalog=aw;Persist Security Info=False;User ID=nicolas;Password=" + services.GetService<IConfiguration>()["dbpassword"] + ";MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;", CancellationToken.None);
    t.Wait();
    return t.Result;
});
builder.Services.AddScoped<TheWheel.ETL.Owin.Middleware>();
TheWheel.ETL.Owin.Middleware.AddJsonFormatter();
TheWheel.ETL.Owin.Middleware.AddCsvFormatter();
// builder.Services.Configure<Microsoft.AspNetCore.Server.Kestrel.Core.KestrelServerOptions>(options =>
//    {
//        options.AllowSynchronousIO = true;
//    })
var app = builder.Build();
app.Map("/data", (Microsoft.AspNetCore.Builder.IApplicationBuilder app1) => app1.UseMiddleware<TheWheel.ETL.Owin.Middleware>());
app.MapGet("/model", (HttpContext context, TheWheel.ETL.Owin.Middleware middleware) => middleware.Model(context, context.RequestAborted));
app.MapGet("/model/clear", (HttpContext context, TheWheel.ETL.Owin.Middleware middleware) => middleware.ClearModelCache(context.RequestAborted));
app.MapGet("/", () => "Hello World!");

app.Run();
