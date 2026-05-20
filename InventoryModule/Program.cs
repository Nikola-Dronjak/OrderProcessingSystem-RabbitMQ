using Common.Messaging.Extensions;
using InventoryModule.Consumers;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddRabbitMQMessaging(builder.Configuration);

builder.Services.AddHostedService<OrderCreatedConsumer>();

var host = builder.Build();
host.Run();
