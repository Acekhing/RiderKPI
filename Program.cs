using Serilog;
using KpiApi.Infrastructure;
using KpiApi.Models;
using KpiApi.Repositories;
using KpiApi.Services;

Log.Logger = new LoggerConfiguration()
    .WriteTo.Console()
    .CreateBootstrapLogger();

try
{
    var builder = WebApplication.CreateBuilder(args);

    builder.Host.UseSerilog((ctx, cfg) => cfg
        .ReadFrom.Configuration(ctx.Configuration)
        .WriteTo.Console());

    // Configuration bindings
    builder.Services.Configure<ClickHouseSettings>(builder.Configuration.GetSection("ClickHouse"));
    builder.Services.Configure<KpiSettings>(builder.Configuration.GetSection("Kpi"));
    builder.Services.Configure<CorsSettings>(builder.Configuration.GetSection("Cors"));
    builder.Services.Configure<GpsPingsProducerSettings>(builder.Configuration.GetSection("GpsPingsProducer"));
    builder.Services.Configure<OrdersProducerSettings>(builder.Configuration.GetSection("OrdersProducer"));

    // DI registrations
    builder.Services.AddSingleton<IKpiRepository, KpiRepository>();
    builder.Services.AddSingleton<IKpiService, KpiService>();
    builder.Services.AddSingleton<GpsPingsClickHouseWriter>();
    builder.Services.AddSingleton<OrdersClickHouseWriter>();
    builder.Services.AddHostedService<GpsPingsBackgroundService>();
    builder.Services.AddHostedService<OrdersBackgroundService>();
    builder.Services.AddControllers();

    // CORS — allow the Next.js dashboard
    var corsOrigins = builder.Configuration
        .GetSection("Cors:AllowedOrigins").Get<string[]>() ?? ["http://localhost:3000"];

    builder.Services.AddCors(o => o.AddDefaultPolicy(p => p
        .WithOrigins(corsOrigins)
        .AllowAnyHeader()
        .AllowAnyMethod()));

    builder.Services.AddEndpointsApiExplorer();
    builder.Services.AddSwaggerGen();
    
    var port = Environment.GetEnvironmentVariable("PORT");
    if (!string.IsNullOrEmpty(port))
    {
        builder.WebHost.UseUrls($"http://0.0.0.0:{port}");
    }

    var app = builder.Build();

    app.UseSerilogRequestLogging();
    app.UseCors();

    if (app.Environment.IsDevelopment())
    {
        app.UseSwagger();
        app.UseSwaggerUI();
    }

    app.MapControllers();

    Log.Information("KPI API starting on {Urls}", builder.Configuration["ASPNETCORE_URLS"] ?? "default");
    app.Run();
}
catch (Exception ex)
{
    Log.Fatal(ex, "KPI API terminated unexpectedly");
}
finally
{
    Log.CloseAndFlush();
}
