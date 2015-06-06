using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace WorkQueues
{
    class WorkQueues
    {
        static void Main(string[] args)
        {
            Worker worker1 = new Worker("1");
            Worker worker2 = new Worker("2");
            Thread.Sleep(1000);
            worker1.Work(null);
            worker2.Work(null);

            NewTask.Work(new string[] { "ena", "..", "duo", "tria" });
            NewTask.Work(new string[] { "First message.    " });
            NewTask.Work(new string[] { "Second message..  " });
            NewTask.Work(new string[] { "Third message...  " });
            NewTask.Work(new string[] { "Fourth message...." });
            NewTask.Work(new string[] { "Fifth message....." });

            Console.ReadKey();
        }
    }

    class NewTask
    {
        public static void Work(string[] args)
        {
            ConnectionFactory factory = new ConnectionFactory() { HostName = "localhost" };
            using (IConnection connection = factory.CreateConnection())
            using (IModel channel = connection.CreateModel())
            {
                channel.QueueDeclare("hello", false, false, false, null);
                string message = GetMessage(args);
                byte[] body = Encoding.UTF8.GetBytes(message);

                IBasicProperties properties = channel.CreateBasicProperties();
                properties.DeliveryMode = 2;
                channel.BasicPublish("", "hello", properties, body);
                Console.WriteLine(" [x] Sent {0}", message);
            }
            //Console.ReadKey();
        }

        private static string GetMessage(string[] args)
        {
            return ((args.Length > 0) ? string.Join(" ", args) : "Hello World!");
        }
    }

    class Worker
    {
        public string Name;
        private BackgroundWorker bw;
        public Worker()
        {
            Name = String.Empty;
            this.bw = new BackgroundWorker();
            this.bw.DoWork += bw_DoWork;
            this.bw.RunWorkerCompleted += bw_RunWorkerCompleted;
        }
        public Worker(string name)
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
                channel.QueueDeclare("hello", false, false, false, null);
                channel.BasicQos(0, 1, false);
                QueueingBasicConsumer consumer = new QueueingBasicConsumer(channel);
                channel.BasicConsume("hello", false, consumer);

                Console.WriteLine(Name + ": [*] Waiting for messages. To exit press CTRL+C");

                while (true)
                {
                    BasicDeliverEventArgs ea = (BasicDeliverEventArgs)consumer.Queue.Dequeue();

                    byte[] body = ea.Body;
                    string message = Encoding.UTF8.GetString(body);
                    Console.WriteLine(Name + ": [x] Received {0}", message);

                    int dots = message.Split('.').Length - 1;
                    Thread.Sleep(dots * 1000);
                    channel.BasicAck(ea.DeliveryTag, false);
                    Console.WriteLine(Name + ": [x] Done with {0}", message);
                }
            }
        }
        private void bw_RunWorkerCompleted(object sender, RunWorkerCompletedEventArgs e)
        {
            Console.WriteLine(Name + ": [x] Completed");
        }
    }
}
