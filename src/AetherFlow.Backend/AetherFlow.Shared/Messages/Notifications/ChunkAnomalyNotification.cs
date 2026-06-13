namespace AetherFlow.Shared.Messages.Notifications;

public record ChunkAnomalyNotification(string Designation, bool NoValue, bool UnknownType) : INotification
{
    public Guid Id { get; init; } = Guid.NewGuid();
    public DateTime CreatedUtc { get; init; } =  DateTime.UtcNow;
}