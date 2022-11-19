using System;
using System.Threading.Tasks;
using Events;


namespace Example.Consumer
{
    class Program
    {
        static async Task Main(string[] args)
        {

            Console.WriteLine("dotnet consumer starting up");

            Console.WriteLine(Environment.GetEnvironmentVariable("SCHEMA_REGISTRY_URL"));

            Console.WriteLine(Environment.GetEnvironmentVariable("BOOTSTRAP_SERVERS"));


            var e = new PullRequestOpened();

            var counter = 0;
            var max = args.Length != 0 ? Convert.ToInt32(args[0]) : -1;
            while (max == -1 || counter < max)
            {
                Console.WriteLine($"Counter: {++counter}");
                await Task.Delay(TimeSpan.FromMilliseconds(1_000));
            }


        }

    }
}
