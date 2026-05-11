using AetherFlow.Domain.Domains;
using AetherFlow.Infrastructure.AetherGenerator;
using AetherFlow.Ingestion.Actors;
using AetherFlow.Shared.AetherInterfaces;
using AetherFlow.Shared.Messages.Ingestion;
using Akka.Actor;
using Akka.Cluster.Hosting;
using Akka.Hosting;
using Akka.Logger.Serilog;
using Akka.Remote.Hosting;
using Serilog;

namespace AetherFlow.Ingestion.ApplicationBuilderConfig;

internal static class ServiceBuildConfig
{
    private const string AkkaSystemName = "AetherFlowIngestion";

    extension(IServiceCollection services)
    {
        private IServiceCollection BuildAkka(string actorSystemName)
        {
            services.AddAkka(actorSystemName, config =>
            {
                // logging -> Serilog
                config.ConfigureLoggers(opt =>
                {
                    opt.ClearLoggers();
                    opt.AddSerilogLogging();
                });
                
                config.WithRemoting(opt =>
                {
                    opt.HostName = "localhost";
                    opt.Port = 9091;
                });

                config.WithClustering(new()
                {
                    Roles = ["aether-ingestion"],
                    SeedNodes = ["akka.tcp://AetherFlow@localhost:9090"]
                });

                // system -> actor-system
                // registry -> out of system // DI container for actors // only for node not for cluster
                // di -> into the system 

                // registry -> actor can be used outside the actor-system // with IActorRegistry -> GetAsync<T>
                // registry -> same with IActorRegistry in actor to use other actor in the system

                // di -> not only into the system also needed to inject into actor 
                // registry to find actor + di to inject actor or class
                config.WithActors((system, registry, di) =>
                {
                    var pipActor = system.ActorOf(di.Props<AetherPipelineActor>());
                    var genSupervisor = system.ActorOf(di.Props<AetherSupervisorActor>());

                    registry.Register<AetherSupervisorActor>(genSupervisor);
                    registry.Register<AetherPipelineActor>(pipActor);

                    genSupervisor.Tell(new StartGenerator());
                });
            });

            return services;
        }

        private IServiceCollection AddServices()
        {
            services.AddScoped<IPeripheryConnector<AetherChunk>, AetherGeneratorSensor>();
            return services;
        }
    }

    extension(HostApplicationBuilder builder)
    {
        
        public HostApplicationBuilder AddLogging()
        {
            Log.Logger = new LoggerConfiguration()
                .ReadFrom.Configuration(builder.Configuration)
                .CreateLogger();

            builder.Logging.ClearProviders();

            return builder;
        }

        public HostApplicationBuilder UseAkka()
        {
            var actorSystemName = builder.GetActorSystemName();
            builder.Services.BuildAkka(actorSystemName);
            return builder;
        }

        public HostApplicationBuilder AddServices()
        {
            builder.Services.AddServices();
            return builder;
        }

        private string GetActorSystemName()
        {
            return builder.Configuration["Akka:ActorSystem"] ?? AkkaSystemName;
        }
    }
}