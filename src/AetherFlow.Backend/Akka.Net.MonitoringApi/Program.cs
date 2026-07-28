using AetherFlow.Shared.Config;
using Akka.Cluster.Hosting;
using Akka.Hosting;
using Akka.Net.Collector.Collector;
using Akka.Remote.Hosting;

var builder = WebApplication.CreateBuilder(args);

// Everything comes from configuration — no hardcoded host/port/seed in code.
var actorSystemName = builder.Configuration["Akka:ActorSystemName"] ?? "AetherFlowCluster";
var settings = builder.Configuration.GetSection("Akka").Get<AkkaSettings>() ?? new AkkaSettings();

builder.Services.AddAkka(actorSystemName, config =>
{
    config
        .WithRemoting(opt =>
        {
            opt.HostName = settings.Remote.Host;
            opt.Port = settings.Remote.Port;
        })
        .WithClustering(new ClusterOptions
        {
            Roles = settings.Cluster.Roles,
            SeedNodes = settings.Cluster.SeedNodes,
            SplitBrainResolver = null,
        })
        .AddMonitoringCollector();
});

var app = builder.Build();

// GET /monitoring/nodes -> latest snapshot per node as JSON
app.MapMonitoringApi();

app.Run();
