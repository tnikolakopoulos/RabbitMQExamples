using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Rpc
{
    class Rpc
    {
        static void Main(string[] args)
        {
            new RPCServer().Work();
            System.Threading.Thread.Sleep(1000);

            RPCClient rpcClient = new RPCClient();

            Console.WriteLine(" [x] Requesting fib(39)");
            string response = rpcClient.Call("39");
            Console.WriteLine(" [.] Got '" + response + "'.");

            Console.WriteLine(" [x] Requesting fib(39)");
            response = rpcClient.Call("39");
            Console.WriteLine(" [.] Got '" + response + "'.");
            
            Console.WriteLine(" [x] Requesting fib(39)");
            response = rpcClient.Call("39");
            Console.WriteLine(" [.] Got '" + response + "'.");

            rpcClient.Close();
        }
    }

    public class RPCServer
    {
        private BackgroundWorker bw;

        public RPCServer()
        {
            bw = new BackgroundWorker();
            bw.DoWork += bw_DoWork;
        }

        public void Work()
        {
            bw.RunWorkerAsync();
        }
        private void bw_DoWork(object sender, DoWorkEventArgs e)
        {
            ConnectionFactory factory = new ConnectionFactory() { HostName = "localhost" };
            using (IConnection connection = factory.CreateConnection())
            using (IModel channel = connection.CreateModel())
            {
                channel.QueueDeclare("rpc_queue", false, false, false, null);
                channel.BasicQos(0, 1, false);
                QueueingBasicConsumer consumer = new QueueingBasicConsumer(channel);
                channel.BasicConsume("rpc_queue", false, consumer);
                Console.Out.WriteLine(" [x] Awaiting RPC requests");
                
                #region My code
                BasicDeliverEventArgs result1;
                //consumer.Queue.Dequeue(100, out result1);
                if (false)
                {
                    //Console.Out.WriteLine(consumer.Queue.Count() + " elements.");
                    for (int i = 0; i < 10; i++)
                    {
                        Console.Out.Write((i + 1) + ". ");
                        BasicDeliverEventArgs result;
                        if (consumer.Queue.Dequeue(100, out result))
                            Console.Out.Write(Encoding.UTF8.GetString(result.Body));
                        Console.Out.WriteLine();
                    }
                }
                #endregion //My code

                while (true)
                {
                    string response = null;
                    BasicDeliverEventArgs ea = (BasicDeliverEventArgs)consumer.Queue.Dequeue();
                    byte[] body = ea.Body;
                    IBasicProperties props = ea.BasicProperties;
                    IBasicProperties replyProps = channel.CreateBasicProperties();
                    replyProps.CorrelationId = props.CorrelationId;

                    try
                    {
                        string message = Encoding.UTF8.GetString(body);
                        int n = Int32.Parse(message);
                        Console.WriteLine(" [.] fib(" + n + ")");
                        response = (n<40) ? fib(n).ToString(): "Ksexna to filaraki, tha parei polli wra";
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(" [.] " + ex.Message);
                        response = String.Empty;
                    }
                    finally
                    {
                        byte[] responseBytes = Encoding.UTF8.GetBytes(response);
                        channel.BasicPublish(String.Empty, props.ReplyTo, replyProps, responseBytes);
                        channel.BasicAck(ea.DeliveryTag, false);
                    }
                }
            }
        }
        private int fib(int n)
        {
            return (n == 0 || n == 1) ? n : (fib(n - 1) + fib(n - 2));
        }
    }

    public class RPCClient
    {
        private IConnection connection;
        private IModel channel;
        private string replyQueueName;
        private QueueingBasicConsumer consumer;

        public RPCClient()
        {
            ConnectionFactory factory = new ConnectionFactory() { HostName = "localhost" };
            connection = factory.CreateConnection();
            channel = connection.CreateModel();
            replyQueueName = channel.QueueDeclare();
            consumer = new QueueingBasicConsumer(channel);
            channel.BasicConsume(replyQueueName, true, consumer);
        }
        public string Call(string message)
        {
            string corrId = Guid.NewGuid().ToString();
            IBasicProperties props = channel.CreateBasicProperties();
            props.ReplyTo = replyQueueName;
            props.CorrelationId = corrId;

            byte[] messageBytes = Encoding.UTF8.GetBytes(message);
            channel.BasicPublish("", "rpc_queue", props, messageBytes);

            while (true)
            {
                BasicDeliverEventArgs ea = consumer.Queue.Dequeue();
                if (ea.BasicProperties.CorrelationId == corrId)
                    return Encoding.UTF8.GetString(ea.Body);

            }
        }
        public void Close()
        {
            connection.Close();
        }
    }
}
