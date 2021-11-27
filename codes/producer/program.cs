using System;
using System.Threading.Tasks;
using Confluent.Kafka;
using Serilog;
using System.Threading;

namespace EnvioKafka
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var logger = new LoggerConfiguration()
                .WriteTo.Console()
                .CreateLogger();
            logger.Information("Testando o envio de mensagens com Kafka");

            // if (args.Length < 3)
            // {
            //     logger.Error(
            //         "Informe ao menos 3 parâmetros: " +
            //         "no primeiro o IP/porta para testes com o Kafka, " +
            //         "no segundo o Topic que receberá a mensagem, " +
            //         "já no terceito em diante as mensagens a serem " +
            //         "enviadas a um Topic no Kafka...");
            //     return;
            // }

            // string bootstrapServers = args[0];
            // string nomeTopic = args[1];

            string bootstrapServers = "localhost:9092";
            string nomeTopic = "my-topic-three-0";
            string[] strings = new string[2] {"ping", "pong"};
            logger.Information($"BootstrapServers = {bootstrapServers}");
            logger.Information($"Topic = {nomeTopic}");
            while(true)
            {
                logger.Information("Waiting");
                Thread.Sleep(1000);
                logger.Information("sending");
                try
                {
                    var config = new ProducerConfig
                    {
                        BootstrapServers = bootstrapServers,
                        QueueBufferingMaxMessages = 2000000
                    };

                    using (var producer = new ProducerBuilder<Null, string>(config).Build())
                    {
                        for (int i = 0; i < strings.Length; i++)
                        {
                            var result = await producer.ProduceAsync(
                                nomeTopic,
                                new Message<Null, string>
                                { Value = strings[i] });

                            logger.Information(
                                $"Mensagem: {strings[i]} | " +
                                $"Status: { result.Status.ToString()}");
                        }
                    }

                    logger.Information("Concluído o envio de mensagens");
                }
                catch (Exception ex)
                {
                    logger.Error($"Exceção: {ex.GetType().FullName} | " +
                                $"Mensagem: {ex.Message}");
                }
            }
        }
    }
}