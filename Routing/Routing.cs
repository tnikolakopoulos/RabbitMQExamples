using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Routing
{
    class Routing
    {
        static void Main(string[] args)
        {

            EmitLogDirect eld = new EmitLogDirect();
            ReceiveLogDirect rld = new ReceiveLogDirect();

            rld.Work(new string[] { "warning", "error" });
            System.Threading.Thread.Sleep(2000);
            eld.Work(new string[] { "error", "error" });
            eld.Work(new string[] { "info", "info" });

            //Console.In.Read();
        }
    }

    public class EmitLogDirect
    {
        public void Work(string[] args)
        {
            ConnectionFactory factory = new ConnectionFactory() { HostName = "localhost" };
            using (IConnection connection = factory.CreateConnection())
            using (IModel channel = connection.CreateModel())
            {
                channel.ExchangeDeclare("direct_logs", "direct");

                string severity = (args.Length > 0) ? args[0] : "info";
                string message = (args.Length > 1) ? String.Join(" ", args.Skip(1).ToArray()) : "Hello world!";
                byte[] body = Encoding.UTF8.GetBytes(message);

                channel.BasicPublish("direct_logs", severity, null, body);
                Console.WriteLine(" [x] Sent '{0}':'{1}'", severity, message);
            }
        }
    }

    public class ReceiveLogDirect
    {
        private BackgroundWorker bw;
        private string[] args;
        public ReceiveLogDirect()
        {
            bw = new BackgroundWorker();
            bw.DoWork += bw_DoWork;
        }
        public void Work(string[] args)
        {
            this.args = args;
            bw.RunWorkerAsync();
        }

        private void bw_DoWork(object sender, DoWorkEventArgs e)
        {
            ConnectionFactory factory = new ConnectionFactory() { HostName = "localhost" };
            using (IConnection connection = factory.CreateConnection())
            using (IModel channel = connection.CreateModel())
            {
                channel.ExchangeDeclare("direct_logs", "direct");
                string queueName = channel.QueueDeclare().QueueName;

                if (args.Length < 1)
                {
                    Console.Error.WriteLine("info, warning or error must be passed as arguments");
                    Environment.Exit(1);
                }
                foreach (string severity in args)
                    channel.QueueBind(queueName, "direct_logs", severity);

                Console.WriteLine(" [*] Waiting for messages. To exit press CTRL+C");

                QueueingBasicConsumer consumer = new QueueingBasicConsumer(channel);
                channel.BasicConsume(queueName, true, consumer);

                while (true)
                {
                    BasicDeliverEventArgs ea = (BasicDeliverEventArgs)consumer.Queue.Dequeue();

                    byte[] body = ea.Body;
                    string message = Encoding.UTF8.GetString(body);
                    string routingKey = ea.RoutingKey;
                    Console.WriteLine(" [x] Received '{0}':'{1}'", routingKey, message);
                }
            }
        }
    }
}
