using BenchmarkModule;
using BenchmarkModule.Consumers;
using BenchmarkModule.Services;
using Common.Messaging.Extensions;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddRabbitMQMessaging(builder.Configuration);

builder.Services.AddSingleton<IMetricsCollectorService, MetricsCollectorService>();

builder.Services.AddHttpClient<IOrderGeneratorService, OrderGeneratorService>(
    client =>
    {
        client.BaseAddress = new Uri("http://order-service:8080");
    });

builder.Services.AddHostedService<NotificationSentConsumer>();
builder.Services.AddHostedService<BenchmarkWorker>();

var host = builder.Build();
host.Run();
