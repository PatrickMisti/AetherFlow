using Akka.Actor;
using Akka.Hosting;
using Moq;

namespace AetherFlow.TestSupport;

/// <summary>
/// Helper for the repeated pattern of mocking <see cref="IRequiredActor{T}"/> so that its
/// <see cref="IRequiredActor{T}.ActorRef"/> resolves to a test probe (or any <see cref="IActorRef"/>).
/// </summary>
public static class RequiredActorMock
{
    /// <summary>Creates a mocked <see cref="IRequiredActor{T}"/> whose ActorRef returns <paramref name="actorRef"/>.</summary>
    public static IRequiredActor<T> For<T>(IActorRef actorRef)
    {
        var mock = new Mock<IRequiredActor<T>>();
        mock.Setup(r => r.ActorRef).Returns(actorRef);
        return mock.Object;
    }
}
