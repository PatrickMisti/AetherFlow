namespace AetherFlow.Shared.AetherInterfaces;

public interface IPeripheryConnector<out T>
{
    void Connect();

    T GenerateData();
    
    void Disconnect();
}