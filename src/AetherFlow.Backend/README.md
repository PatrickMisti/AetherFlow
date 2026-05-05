Projekt,Typ,Zweck
AetherFlow.Shared,Class Lib,Messages & Protobuf (Der „Vertrag“).
AetherFlow.Domain,Class Lib,Logik & Mathematik der Sensoren.
AetherFlow.Infrastructure,Class Lib,Datenbank-Logik & gRPC-Clients.
AetherFlow.Producer,Worker Service,Der Generator mit den Loop-Actors.
AetherFlow.Engine,Worker Service,Der Akka Cluster & die TPL Pipeline.


IoC in actor

DependencyResolver.For(Context.System)

Outside Service

IServiceProvider sp inject and now GetRequiredService

or IServiceScopeFactory