using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HelloWorld
{
    class HelloWorld
    {
        static void Main(string[] args)
        {
            Send.Work(null);
            Receive.Work(null);
        }
    }

    class Send
    {
        public static void Work(string[] args)
        {
            ConnectionFactory factory = new ConnectionFactory() { HostName = "localhost" };
            using (IConnection connection = factory.CreateConnection())
            using (IModel channel = connection.CreateModel())
            {
                channel.QueueDeclare("hello", false, false, false, null);
                string message = "Hello World!";
                byte[] body = Encoding.UTF8.GetBytes(message);

                channel.BasicPublish("", "hello", null, body);
                Console.WriteLine(" [x] Sent {0}", message);
            }
            //Console.ReadKey();
        }
    }

    class Receive
    {
        public static void Work(string[] args)
        {
            ConnectionFactory factory = new ConnectionFactory() { HostName = "localhost" };
            using (IConnection connection = factory.CreateConnection())
            using (IModel channel = connection.CreateModel())
            {
                channel.QueueDeclare("hello", false, false, false, null);
                QueueingBasicConsumer consumer = new QueueingBasicConsumer(channel);
                channel.BasicConsume("hello", true, consumer);

                Console.WriteLine(" [*] Waiting for messages. To exit press CTRL+C");

                while (true)
                {
                    BasicDeliverEventArgs ea = (BasicDeliverEventArgs)consumer.Queue.Dequeue();

                    byte[] body = ea.Body;
                    string message = Encoding.UTF8.GetString(body);
                    Console.WriteLine(" [x] Received {0}", message);
                }
            }
        }
    }
}
