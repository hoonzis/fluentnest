using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using FluentNest.Tests.Model;
using Nest;
using NFluent;

namespace FluentNest.Tests
{
    public class TestsBase
    {
        protected ElasticClient Client;

        private readonly Dictionary<string, string> testResults;
        public TestsBase(params Func<ConnectionSettings, ConnectionSettings>[] additionalSettings)
        {
            var node = new Uri("http://localhost:9200");

            Console.WriteLine("Createing new instance of connection settings");
            var settings = new ConnectionSettings(
                node,
                defaultIndex: "my-application"
                );

            if (additionalSettings.Length > 0)
            {
                Console.WriteLine("Additional converters will be applied");
            }else
            {
                Console.WriteLine("No additional converters applied - the enums should be serialized as integers");
            }

            settings = additionalSettings.Aggregate(settings, (current, newSetting) => newSetting(current));

            Client = new ElasticClient(settings);

            testResults = LoadTestResults(this.GetType().Name);
        }

        public void AddSimpleTestData()
        {
            Client.DeleteIndex(x => x.Index<Car>());
            Client.CreateIndex(c => c.Index<Car>().AddMapping<Car>(x => x
            .Properties(prop => prop.String(str => str.Name(s => s.EngineType).Index(FieldIndexOption.NotAnalyzed)))));

            for (int i = 0; i < 10; i++)
            {
                var car = new Car
                {
                    Timestamp = new DateTime(2010, i + 1, 1),
                    Name = "Car" + i,
                    Price = 10,
                    Sold = i % 2 == 0 ? true : false,
                    CarType = "Type" + i % 3,
                    Length = i,
                    EngineType = i % 2 == 0 ? EngineType.Diesel : EngineType.Standard,
                    Weight = 5,
                    ConditionalRanking = i%2 ==0 ? null : (int?)i,
                    Description = "Desc" + i,
                    LongField = i,
                    IntField = i
                };
                Client.Index(car);
            }
            Client.Flush(x => x.Index<Car>());
        }

        public string Serialize<T>(T entity)
        {
            var bytes = Client.Serializer.Serialize(entity);
            return Encoding.UTF8.GetString(bytes);
        }

        public Dictionary<string, string> LoadTestResults(string className)
        {
            var fileName = className + ".txt";
            if (!File.Exists(fileName))
            {
                return new Dictionary<string, string>();
            }

            var testLines  = File.ReadAllText(className + ".txt").Split("###".ToCharArray()).Select(x=>x.Trim());
            var values = testLines.Where(x=>x.Contains("***")).Select(x=>x.Trim()).Select(x =>
            {
                var testContent = x.Split("***".ToCharArray()).Where(y => !string.IsNullOrWhiteSpace(y)).ToArray();
                return new
                {
                    Name = testContent[0].Trim(),
                    Json = testContent[1].Trim()
                };
            });

            return values.ToDictionary(x => x.Name, y => y.Json);
        }

        public void CheckSD<T>(SearchDescriptor<T> sc, string testName) where T: class
        {
            var json = Serialize(sc);
            Check.That(json).Equals(testResults[testName]);
        } 
    }
}
