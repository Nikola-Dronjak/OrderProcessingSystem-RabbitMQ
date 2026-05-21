namespace Common.Commands
{
    public abstract class BaseCommand
    {
        public string CorrelationId { get; set; }
        public DateTime Timestamp { get; set; }
    }
}
