using AetherFlow.Ingestion.ApplicationBuilderConfig;

Host
    .CreateApplicationBuilder(args)
    .AddLogging()
    .AddServices()
    .UseAkka()
    .Build()
    .Run();
