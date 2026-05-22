namespace Common.Messaging.RabbitMQ
{
    public static class RabbitMQConstants
    {
        // Exchange names:
        public const string ExchangeName = "order-processing";

        // Routing keys:
        public const string OrderCreatedRoutingKey = "order-created";
        public const string InventoryReservedRoutingKey = "inventory-reserved";
        public const string ProcessPaymentRoutingKey = "process-payment";
        public const string PaymentSucceededRoutingKey = "payment-succeeded";
        public const string OrderCompletedRoutingKey = "order-completed";
    }
}
