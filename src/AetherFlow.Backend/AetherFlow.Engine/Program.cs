using AetherFlow.Backend.ServiceDefaults;
using AetherFlow.Engine;
using AetherFlow.Engine.ApplicationBuilderConfig;

var builder = Host.CreateApplicationBuilder(args);

builder
    .AddServiceDefaults()
    .WithEngineWorker();

// builder.Services.AddHostedService<Worker>();
var host = builder.Build();
host.Run();