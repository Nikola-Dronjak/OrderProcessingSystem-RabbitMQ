namespace OrderModule.Messaging
{
    public interface IMessageBus
    {
        Task PublishAsync<T>(string topic, T message, CancellationToken cancellationToken);
    }
}
