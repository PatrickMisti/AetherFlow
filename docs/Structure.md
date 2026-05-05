# Root-Structure

/AetherFlow (Repository Root)
├── /docs                 (Architektur-Diagramme, Konzepte)
├── /infra                (Docker-Compose, K8s-Manifeste)
├── /src
│   ├── /AetherFlow.Backend (Die .NET Welt)
│   │   ├── AetherFlow.Backend.sln
│   │   ├── /AetherFlow.Ingestion (Traffic Generator & TPL)
│   │   ├── /AetherFlow.Engine    (Akka.NET Cluster)
│   │   └── /AetherFlow.Shared    (Protobuf/Models)
│   ├── /AetherFlow.Control (Die Elixir Welt)
│   │   ├── /ash_domain
│   │   ├── /phoenix_ui
│   │   └── mix.exs
└── README.md             (Das Aushängeschild)

# Backend-Structure
| Projekt | Typ | Zweck |
| --- | --- | --- |
| AetherFlow.Shared | Class Lib | Messages & Protobuf (Der „Vertrag“). |
| AetherFlow.Domain | Class Lib | Logik & Mathematik der Sensoren. |
| AetherFlow.Infrastructure | Class Lib | Datenbank-Logik & gRPC-Clients. |
| AetherFlow.Producer | Worker Service | Der Generator mit den Loop-Actors. |
| AetherFlow.Engine | Worker Service | Der Akka Cluster & die TPL Pipeline. |


## Generell

1. Architektur-Struktur (Solution)
AetherFlow.Shared: Messages, Protobuf-Verträge & Konstanten (der "Single Source of Truth"-Anker).

AetherFlow.Domain: Reine Geschäftslogik (Anomalie-Berechnungen, Sensor-Regeln) – ohne Framework-Abhängigkeit.

AetherFlow.Infrastructure: Technische Implementierungen (Datenbank/Persistence, gRPC-Clients, Logging).

AetherFlow.Producer: C# Worker Service; Supervisor steuert Sensor-Actors, die Daten-Loops (Tasks) für die TPL-Eingabe betreiben.

AetherFlow.Engine: Das Herzstück; TPL-Pipeline -> Akka.Streams -> Akka.Cluster Sharding (Stateful Actors).

AetherFlow.Control: Elixir/Ash App für Business-Logik-Verwaltung & Phoenix LiveView Dashboard.

2. Der Datenfluss (The Flow)
Ingestion: Producer (C#) erzeugt Daten -> SendAsync (Backpressure) -> TPL BufferBlock.

Processing: TPL-Worker -> Akka Stream (Batching/Filter) -> Shard Region.

State: Sensor-Actor (C#) prüft Zustand & speichert via Akka Persistence.

Integration: gRPC-Bridge meldet Events an Elixir & empfängt Steuerbefehle (Drosselung).

3. "Senior" Keywords für die README
Reactive Manifest: (Responsive, Resilient, Elastic, Message Driven).

Backpressure: Durchgängige Drosselung vom Cluster bis zum Generator.

Hybrid Concurrency: Kombination aus Actors (State) und TPL (Throughput).

Polyglot Distributed System: C# für Heavy-Duty, Elixir für DX/Control-Plane.

4. Nächste Schritte (Action Items)
Shared Lib mit Protobuf & Records erstellen.

Producer mit Supervisor & Loop-Actors bauen.

TPL-zu-Akka-Bridge (Source.Queue) implementieren.

gRPC-Service definieren, um die Welten zu koppeln.