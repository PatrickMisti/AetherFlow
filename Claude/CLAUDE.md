# AetherFlow – Architecture Guide for Claude

## Project Overview

AetherFlow is a **polyglot distributed system** for real-time sensor data processing.
It combines a **C# .NET backend** (heavy-duty processing) with an **Elixir control plane**
(business logic & live dashboard). The design follows the Reactive Manifest:
*Responsive, Resilient, Elastic, Message Driven*.

---

## Repository Structure

```
/AetherFlow
├── /docs          # Architecture diagrams, concepts
├── /infra         # Docker-Compose, Kubernetes manifests
├── /src
│   ├── /AetherFlow.Backend      # .NET world
│   │   ├── AetherFlow.Backend.sln
│   │   ├── /AetherFlow.Shared       # Protobuf / Models (contract)
│   │   ├── /AetherFlow.Domain       # Business logic & math
│   │   ├── /AetherFlow.Infrastructure # DB & gRPC clients
│   │   ├── /AetherFlow.Ingestion     # Worker Service – Sensor Actors
│   │   └── /AetherFlow.Engine       # Akka.NET Cluster & TPL Pipeline
│   └── /AetherFlow.Control          # Elixir world
│       ├── /ash_domain
│       ├── /phoenix_ui
│       └── mix.exs
└── README.md
```

---

## Backend Projects

| Project | Type | Purpose |
|---|---|---|
| `AetherFlow.Shared` | Class Library | Protobuf contracts, Messages, Constants – Single Source of Truth |
| `AetherFlow.Domain` | Class Library | Pure business logic, anomaly detection, sensor rules – no framework deps |
| `AetherFlow.Infrastructure` | Class Library | DB/persistence, gRPC clients, logging |
| `AetherFlow.Ingestion` | Worker Service | Supervisor + Loop-Actors → TPL BufferBlock input |
| `AetherFlow.Engine` | Worker Service | TPL Pipeline → Akka.Streams → Akka.Cluster Sharding |

---

## Data Flow

```
Ingestion (C#)
  └─► SendAsync [Backpressure] ──► TPL BufferBlock
                                        │
                                   TPL Worker
                                        │
                                  Akka Stream
                               (Batching / Filter)
                                        │
                                  Shard Region
                                        │
                               Sensor Actor (C#)
                           (State + Akka Persistence)
                                        │
                                  gRPC Bridge
                                   ┌───┴───┐
                              Events ↑   ↓ Control Commands
                                  Elixir / Ash
                               Phoenix LiveView
```

**Key principle – Backpressure is end-to-end:**
From the Akka.Cluster all the way back to the Generator, every stage applies
`SendAsync` / bounded channels so no stage can overwhelm the next.

---

## Concurrency Model

| Concern | Technology | Why |
|---|---|---|
| State management | Akka.NET Actors | Location-transparent, fault-tolerant, stateful |
| Throughput / batching | TPL Dataflow | High-performance pipeline, composable blocks |
| Control plane | Elixir / OTP | Lightweight processes, hot-code reload, great DX |
| Cross-world bridge | gRPC (Protobuf) | Strongly-typed, binary, bidirectional streaming |

> **Hybrid Concurrency**: Actors own *state*, TPL owns *throughput*. Neither is used where the other excels.

---

## Strict Layer Rules (Most Important)

This project enforces **strict layer separation**. Violating these rules is never acceptable.

### Forbidden Dependencies

| Project | Must NOT reference |
|---|---|
| `AetherFlow.Domain` | Infrastructure, Akka, EF, gRPC, any I/O |
| `AetherFlow.Shared` | Any other project in this solution |
| `AetherFlow.Ingestion` | `AetherFlow.Engine` |
| `AetherFlow.Engine` | `AetherFlow.Ingestion` |

### Layer Flow (only downward allowed)

```
Ingestion / Engine
      │
 Infrastructure
      │
    Domain
      │
    Shared
```

### When adding a new feature, always start from the bottom:
1. `Shared` – define the contract (Protobuf / record)
2. `Domain` – implement pure logic
3. `Infrastructure` – add persistence / gRPC if needed
4. `Ingestion` / `Engine` – wire it up

---

## Coding Conventions

### C#
- Records over classes for data (immutable by default)
- Async all the way – never `.Result` or `.Wait()`
- Primary constructors preferred
- Actors: suffix `Actor` (e.g. `SensorActor`, `SupervisorActor`)
- TPL Blocks: suffix by type (e.g. `_buffer`, `_transform`, `_broadcast`)
- Proto messages: PascalCase, suffix `Request` / `Response` / `Event`

### Elixir
- Ash resources for all domain entities
- No raw Ecto queries outside Infrastructure
- Pattern match over if/else
- Module naming: full namespace (e.g. `AetherFlow.Domain.Sensor`)

---

## Testing

### Framework & Tools
- **NUnit** as test framework (`[TestFixture]`, `[Test]`, `[SetUp]`)
- **Akka.TestKit.NUnit** for Actor tests – extend `TestKit`, use `CreateTestProbe()`
- **Moq** for mocking dependencies (e.g. `IRequiredActor<T>`)

### Conventions
- Test project naming: `AetherFlow.<Project>Tests` (e.g. `AetherFlow.IngestionTests`)
- Test class naming: `<ActorName>Test` grouped in folder `<ActorName>Tests/`
- Each test is **self-contained** – own `ActorOf` per test, unique actor name per test
- Dependencies (e.g. pipeline actor) are mocked via `Mock<IRequiredActor<T>>` + `CreateTestProbe()`
- `[SetUp]` initializes mocks and probes before each test

### Structure Example
```csharp
[TestFixture]
public class AetherSupervisorTest : TestKit
{
    private Mock<IRequiredActor<AetherPipelineActor>> _mockPipeline = null!;

    [SetUp]
    public void SetUp()
    {
        var pipelineProbe = CreateTestProbe();
        _mockPipeline = new Mock<IRequiredActor<AetherPipelineActor>>();
        _mockPipeline.Setup(p => p.ActorRef).Returns(pipelineProbe.Ref);
    }

    [Test]
    public void AetherSupervisor_ShouldSpawnCorrectNumberOfWorkers()
    {
        var props = Props.Create(() => new AetherSupervisor(_mockPipeline.Object, workers: 4));
        var supervisor = Sys.ActorOf(props, "supervisor-spawn-test");
        Assert.NotNull(supervisor);
    }
}
```

### What to test
- Actor spawns without exception
- Messages are dispatched and processed without throwing
- Pure logic (e.g. grouping, aggregation) tested directly without actors
- Edge cases: empty lists, zero workers, multiple dispatches

### What NOT to test
- Internal actor state directly (use message-based assertions)
- Infrastructure layer with in-memory fakes

---

## Infrastructure & Deployment

- **Local**: Docker Compose only
- Database: **not yet decided** – defer DB-specific choices, keep persistence behind a repository interface
- Do not hardcode connection strings – always use environment variables / config

---

## Claude – Behavior Rules

### Language
- **Code**: English (identifiers, comments, commit messages)
- **Explanations**: German

### Architecture Decisions
- **Always ask before implementing** architectural changes
- If a task touches layer boundaries, new packages, or project structure → stop and ask first
- Small implementation details within an existing pattern → just do it

### Code Output
- Focus on correctness over format
- Partial diffs or full files – whatever is clearest for the change

### Don't
- Don't add new NuGet packages without asking
- Don't put logic in `Ingestion` or `Engine` – it belongs in `Domain`
- Don't use `dynamic` or `object` as message types
- Don't create new `.proto` messages without updating both C# and Elixir sides
- Don't reference upward in the layer hierarchy

---

## Action Items

- [ ] Create `AetherFlow.Shared` with Protobuf definitions & C# records
- [ ] Build `AetherFlow.Ingestion` – Supervisor + Loop-Actors → `BufferBlock<T>`
- [ ] Implement TPL-to-Akka bridge via `Source.Queue`
- [ ] Define gRPC service (`.proto`) for C# ↔ Elixir communication
- [ ] Wire `AetherFlow.Infrastructure` with persistence (repository pattern, DB TBD)
- [ ] Bootstrap Elixir Ash domain + Phoenix LiveView dashboard

---

## Glossary

| Term | Meaning |
|---|---|
| **Backpressure** | Upstream slows down when downstream is busy – prevents buffer overflow |
| **Shard Region** | Akka.Cluster entry point that routes messages to the correct node/actor |
| **TPL** | Task Parallel Library – `System.Threading.Tasks.Dataflow` |
| **Ash** | Elixir resource framework (declarative, extensible domain modeling) |
| **gRPC Bridge** | The `AetherFlow.Infrastructure` service connecting .NET ↔ Elixir |
