namespace AetherFlow.Shared.Messages.Notifications;

public class ManifestationStateAbsentNotification : INotification
{
    public Guid Id { get; } = Guid.NewGuid();
    private ManifestationStateAbsentNotification() { }

    public static ManifestationStateAbsentNotification Instance => new();
}