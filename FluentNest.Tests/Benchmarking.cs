using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using FluentNest.Tests.Model;
using Nest;
using NFluent;
using Xunit;

namespace FluentNest.Tests
{
    public class Benchmarking : TestsBase
    {
        private void AddSimpleTestData(int iterations)
        {
            Client.DeleteIndex(x => x.Index<Car>());
            Client.CreateIndex(c => c.Index<Car>().AddMapping<Car>(x => x
            .Properties(prop => prop.String(str => str.Name(s => s.EngineType).Index(FieldIndexOption.NotAnalyzed)))));

            for (int j = 0; j < iterations; j++)
            {
                var cars = new List<Car>();
                for (int i = 0; i < 1000; i++)
                {
                    var car = new Car
                    {
                        Timestamp = new DateTime(2010, (i%12) + 1, 1),
                        Name = "Car" + i,
                        Price = 10,
                        Sold = i%2 == 0,
                        CarType = "Type" + i%2,
                        Emissions = i + 1,
                        IntField = i
                    };
                    cars.Add(car);                   
                }
                Client.Bulk(x => x.CreateMany(cars));
            }

            
            Client.Flush(x => x.Index<Car>());
        }

        // This should confirm the theory that a two side range is faster then a bool with 2-one sided ranges
        [Fact]
        public void WithMergedRange()
        {
            var stopWatch = new Stopwatch();
            AddSimpleTestData(5);
            var sc = new SearchDescriptor<Car>().FilterOn(x => x.Emissions > 2 && x.Emissions < 6 && x.Price < 20);
            var json = Encoding.UTF8.GetString(Client.Serializer.Serialize(sc));
            Console.WriteLine(json);

            stopWatch.Start();
            var allCars = Client.Search<Car>(sc);
            stopWatch.Stop();

            Console.WriteLine("Query time:" + stopWatch.Elapsed);
            Check.That(allCars.Documents).HasSize(10);
        }

        [Fact]
        public void WithoutMergedRange()
        {
            var stopWatch = new Stopwatch();
            AddSimpleTestData(5);
            var sc = new SearchDescriptor<Car>().FilterOn(x => x.Emissions > 2 && x.Price < 20 && x.Emissions < 6);
            var json = Encoding.UTF8.GetString(Client.Serializer.Serialize(sc));
            Console.WriteLine(json);

            stopWatch.Start();
            var allCars2 = Client.Search<Car>(sc);
            stopWatch.Stop();

            Console.WriteLine("Query time:" + stopWatch.Elapsed);
            Check.That(allCars2.Documents).HasSize(10);
        }


        [Fact]
        public void WithoutMergedAndFilters()
        {
            var stopWatch = new Stopwatch();
            AddSimpleTestData(5);
            Filters.OptimizeAndFilters = false;

            var sc =
                new SearchDescriptor<Car>().FilterOn(
                    x =>
                        x.Emissions < 6 && x.Sold == true && x.Price > 4 && x.EngineType == EngineType.Diesel &&
                        x.Length < 4);

            var json = Encoding.UTF8.GetString(Client.Serializer.Serialize(sc));
            Console.WriteLine(json);

            stopWatch.Start();
            var allCars = Client.Search<Car>(sc);
            stopWatch.Stop();

            Console.WriteLine("Query time:" + stopWatch.Elapsed); Console.Write("Query time:" + stopWatch.Elapsed);
            Check.That(allCars.Documents).HasSize(10);
        }

        [Fact]
        public void WithMergedAndFilters()
        {
            var stopWatch = new Stopwatch();
            AddSimpleTestData(5);
            var sc =
                new SearchDescriptor<Car>().FilterOn(
                    x =>
                        x.Emissions < 6 && x.Sold == true && x.Price > 4 && x.EngineType == EngineType.Diesel &&
                        x.Length < 4);

            var json = Encoding.UTF8.GetString(Client.Serializer.Serialize(sc));
            Console.WriteLine(json);

            stopWatch.Start();
            var cars = Client.Search<Car>(sc);
            stopWatch.Stop();

            Console.WriteLine("Query time:" + stopWatch.Elapsed);
            Check.That(cars.Documents).HasSize(10);
        }
    }
}