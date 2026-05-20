using AetherFlow.Backend.ServiceDefaults;
using AetherFlow.Ingestion.ApplicationBuilderConfig;

Host
    .CreateApplicationBuilder(args)
    .AddServiceDefaults()
    .AddServices()
    .UseAkka()
    .Build()
    .Run();