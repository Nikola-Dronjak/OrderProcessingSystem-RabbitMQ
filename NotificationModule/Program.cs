using Common.Messaging.Extensions;
using NotificationModule.Consumers;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddRabbitMQMessaging(builder.Configuration);

builder.Services.AddHostedService<OrderCompletedConsumer>();

var host = builder.Build();
host.Run();
