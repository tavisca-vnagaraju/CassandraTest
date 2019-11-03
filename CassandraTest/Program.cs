using System;
using System.Diagnostics;
using System.Threading.Tasks;
using Cassandra;

namespace Instaclustr.Cassandra.ConnectionSample
{
    public class MainClass
    {
        public void GetAllProducts()
        {
            var session = connectCluster("txs");
            // Execute a query on a connection synchronously
            var rs = session.Execute("SELECT * FROM product");
            // Iterate through the RowSet
            foreach (var row in rs)
            {
                var value = row.GetValue<string>("name");
                Console.WriteLine(value);
            }
        }
        public void GetProductByName(string name)
        {
            var session = connectCluster("txs");
            // Execute a query on a connection synchronously
            var QueryString = "SELECT * FROM product WHERE name=" + "'" + name + "'";
            Stopwatch stopWatch = new Stopwatch();

            var tasks = new Task[]
            {
                Task.Factory.StartNew(() => Find(session, QueryString)),
                Task.Factory.StartNew(() => Find(session, QueryString)),
                Task.Factory.StartNew(() => Find(session, QueryString)),
                Task.Factory.StartNew(() => Find(session, QueryString)),
                Task.Factory.StartNew(() => Find(session, QueryString))
            };
            stopWatch.Start();
            Task.WaitAll(tasks);
            stopWatch.Stop();
            Console.WriteLine("Cassandra Filter By Name- Time:" + stopWatch.ElapsedMilliseconds);
            //foreach (var row in rs)
            //{
            //    var value = row.GetValue<string>("name");
            //    Console.WriteLine(value);
            //}
        }
        public static void Find(ISession session,string QueryString)
        {
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
            session.Execute(QueryString);
            stopwatch.Stop();
            Console.WriteLine("Stop Cassandra Indivudual Time: " + stopwatch.ElapsedMilliseconds);
            stopwatch.Reset();
        }
        public void InsertOneProduct()
        {
            var session = connectCluster("txs");
            var rs = session.Execute("INSERT INTO product (id, name, price , status ) VALUES( 1,'product1', 46578, 'active')");
        }
        public void InsertManyProducts()
        {
            var session = connectCluster("txs");
            for (int i = 1; i < 200000; i++)
            {
                var valuesString = "(" + i.ToString() + ",'" + "product" + i + "'," + (i * 2 + 8).ToString() + ",'active')";
                var QueryInsertString = "INSERT INTO small_product (id, name, price , status ) VALUES" + valuesString;
                Console.WriteLine(QueryInsertString);
                session.Execute(QueryInsertString);
            }
        }
        public ISession connectCluster(string clusterName)
        {
            var cluster = Cluster.Builder()
                     .AddContactPoints("localhost")
                     .Build();

            // Connect to the nodes using a keyspace
            var session = cluster.Connect(clusterName);
            return session;
        }
        public static void Main(string[] args)
        {
            

            MainClass mainClass = new MainClass();

            mainClass.GetProductByName("product10000");
            //mainClass.InsertManyProducts();
            
            

            //Hold the screen by logic  
            Console.WriteLine("Press any key to terminate the program");
            Console.ReadKey();
        }
    }
}