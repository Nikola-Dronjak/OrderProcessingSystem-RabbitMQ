namespace Common.Messaging.RabbitMQ
{
    public static class RabbitMQConstants
    {
        #region Exchange names
        public const string ExchangeName = "order-processing";
        #endregion

        #region Routing keys
        public const string OrderCreatedRoutingKey = "order-created";
        public const string InventoryReservedRoutingKey = "inventory-reserved";
        public const string ProcessPaymentRoutingKey = "process-payment";
        public const string PaymentSucceededRoutingKey = "payment-succeeded";
        public const string PaymentFailedRoutingKey = "payment-failed";
        public const string PaymentDeadLetterRoutingKey = "payment-dead-letter";
        public const string OrderCompletedRoutingKey = "order-completed";
        public const string NotificationSentRoutingKey = "notification-sent";
        #endregion
    }
}
