using AetherFlow.Backend.ServiceDefaults;
using AetherFlow.Engine.ApplicationBuilderConfig;

Host
    .CreateApplicationBuilder(args)
    .AddServiceDefaults()
    .WithEngineWorker()
    .Build()
    .Run();