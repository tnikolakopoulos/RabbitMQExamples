using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Topics
{
    class Topics
    {
        static void Main(string[] args)
        {
            //foreach (string arg in args)
            //    Console.Out.WriteLine(arg);
            //Console.Out.WriteLine("-----------------------------------");

            EmitLogTopic elt = new EmitLogTopic();
            ReceiveLogTopic rlt1 = new ReceiveLogTopic();
            ReceiveLogTopic rlt2 = new ReceiveLogTopic();
            ReceiveLogTopic rlt3 = new ReceiveLogTopic();
            rlt1.Work(new string[] { "#" });
            rlt2.Work(new string[] { "" });
            rlt3.Work(new string[] { "#.*" });
            System.Threading.Thread.Sleep(1000);

            elt.Work(new string[] { "kern.critical", "1. A critical kernel error" });
            elt.Work(new string[] { "", "2. Empty" });
            elt.Work(new string[] { "..", "3. Double dot" });
            elt.Work(new string[] { "kern.critical", "4. A critical kernel error" });
            elt.Work(new string[] { "kern.critical", "5. A critical kernel error" });
            elt.Work(new string[] { "kern.critical", "6. A critical kernel error" });

            Console.In.Read();
        }
    }

    public class EmitLogTopic
    {
        public void Work(string[] args)
        {
            ConnectionFactory factory = new ConnectionFactory() { HostName = "localhost" };
            using (IConnection connection = factory.CreateConnection())
            using (IModel channel = connection.CreateModel())
            {
                channel.ExchangeDeclare("topic_logs", "topic");

                string routingKey = (args.Length > 0) ? args[0] : "anonymous.info";
                string message = (args.Length > 1) ? String.Join(" ", args.Skip(1).ToArray())
                    : "Hello world!";
                byte[] body = Encoding.UTF8.GetBytes(message);
                channel.BasicPublish("topic_logs", routingKey, null, body);
                Console.Out.WriteLine("[x] Sent '{0}':'{1}'", routingKey, message);

            }
        }
    }

    public class ReceiveLogTopic
    {
        private BackgroundWorker bw;

        public ReceiveLogTopic()
        {
            this.bw = new BackgroundWorker();
            bw.DoWork += bw_DoWork;
        }

        void bw_DoWork(object sender, DoWorkEventArgs e)
        {
            string[] args = e.Argument as string[];

            ConnectionFactory factory = new ConnectionFactory() { HostName = "localhost" };
            using (IConnection connection = factory.CreateConnection())
            using (IModel channel = connection.CreateModel())
            {
                channel.ExchangeDeclare("topic_logs", "topic");
                string queueName = channel.QueueDeclare().QueueName;

                if (args.Length < 1)
                {
                    Console.Error.WriteLine("binding key(s) must be provided");
                    Environment.Exit(1);
                }

                foreach (string bindingKey in args)
                    channel.QueueBind(queueName, "topic_logs", bindingKey);

                Console.Out.WriteLine(" [*] Waiting for messages. To exit press Ctrl+C");
                QueueingBasicConsumer consumer = new QueueingBasicConsumer(channel);
                channel.BasicConsume(queueName, true, consumer);

                while (true)
                {
                    BasicDeliverEventArgs ea = (BasicDeliverEventArgs)consumer.Queue.Dequeue();
                    byte[] body = ea.Body;
                    string message = Encoding.UTF8.GetString(body);
                    string routingKey = ea.RoutingKey;
                    Console.Out.WriteLine(" [x] Received '{0}':'{1}' (my topic is: '{2}')", routingKey, message, String.Join(" ", args));
                }
            }
        }

        public void Work(string[] args)
        {
            this.bw.RunWorkerAsync(args);
        }

    }
}
