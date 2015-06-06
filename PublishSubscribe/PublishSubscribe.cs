using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace PublishSubscribe
{
    class PublishSubscribe
    {
        static void Main(string[] args)
        {
            ReceiveLogs worker1 = new ReceiveLogs("1");
            ReceiveLogs worker2 = new ReceiveLogs("2");
            worker1.Work(null);
            worker2.Work(null);
            
            Thread.Sleep(1000);
            
            EmitLog.Work(new string[] { "ena", "..", "duo", "tria" });
            EmitLog.Work(new string[] { "First message.    " });
            EmitLog.Work(new string[] { "Second message..  " });
            EmitLog.Work(new string[] { "Third message...  " });
            EmitLog.Work(new string[] { "Fourth message...." });
            EmitLog.Work(new string[] { "Fifth message....." });

            Console.ReadKey();
        }
    }

    class EmitLog
    {
        public static void Work(string[] args)
        {
            ConnectionFactory factory = new ConnectionFactory() { HostName = "localhost" };
            using (IConnection connection = factory.CreateConnection())
            using (IModel channel = connection.CreateModel())
            {
                channel.ExchangeDeclare("logs", "fanout");
                string message = GetMessage(args);
                byte[] body = Encoding.UTF8.GetBytes(message);

                channel.BasicPublish("logs", "", null, body);
                Console.WriteLine(" [x] Sent {0}", message);
            }
            //Console.ReadKey();
        }

        private static string GetMessage(string[] args)
        {
            return ((args.Length > 0) ? string.Join(" ", args) : "info: Hello World!");
        }
    }

    class ReceiveLogs
    {
        public string Name;
        private BackgroundWorker bw;
        public ReceiveLogs()
        {
            Name = String.Empty;
            this.bw = new BackgroundWorker();
            this.bw.DoWork += bw_DoWork;
            //this.bw.RunWorkerCompleted += bw_RunWorkerCompleted;
        }
        public ReceiveLogs(string name)
            : this()
        {
            this.Name = name;
        }
        public void Work(string[] args)
        {
            bw.RunWorkerAsync(args);
        }
        private void bw_DoWork(object sender, DoWorkEventArgs dwea)
        {
            ConnectionFactory factory = new ConnectionFactory() { HostName = "localhost" };
            using (IConnection connection = factory.CreateConnection())
            using (IModel channel = connection.CreateModel())
            {
                channel.ExchangeDeclare("logs", "fanout");
                string queueName = channel.QueueDeclare();
                channel.QueueBind(queueName, "logs", "");
                channel.BasicQos(0, 1, false);
                QueueingBasicConsumer consumer = new QueueingBasicConsumer(channel);
                channel.BasicConsume(queueName, true, consumer);

                Console.WriteLine(" [*] Waiting for logs. To exit press CTRL+C");

                while (true)
                {
                    BasicDeliverEventArgs ea = (BasicDeliverEventArgs)consumer.Queue.Dequeue();

                    byte[] body = ea.Body;
                    string message = Encoding.UTF8.GetString(body);
                    Console.WriteLine(" [x] {0}", message);
                }
            }
        }
        private void bw_RunWorkerCompleted(object sender, RunWorkerCompletedEventArgs e)
        {
            Console.WriteLine(" [x] Completed");
        }
    }
}
