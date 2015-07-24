using System;
using FluentNest;
using Nest;
using NFluent;
using TestModel;
using Xunit;

namespace Tests
{
    public class FilterTests
    {
        private ElasticClient client;

        public FilterTests()
        {
            var node = new Uri("http://localhost:9600");

            var settings = new ConnectionSettings(
                node,
                defaultIndex: "my-application"
            );

            client = new ElasticClient(settings);
        }

        private void AddSimpleTestData()
        {
            client.DeleteIndex(x => x.Index<Car>());
            for (int i = 0; i < 10; i++)
            {
                var car = new Car
                {
                    Timestamp = new DateTime(2010,(i%12)+1,1),
                    Name = "Car" + i,
                    Price = 10,
                    Sold = i % 2 == 0 ? true : false,
                    CarType = "Type" + i%2
                };
                client.Index(car);
            }
            client.Flush(x => x.Index<Car>());
        }


        [Fact]
        public void DateComparisonAndTerm()
        {
            AddSimpleTestData();
            var startDate = new DateTime(2010, 1, 1);
            var endDate = new DateTime(2010, 3, 1);
            var carType = "Type0";

            var result = client.Search<Car>(s => s
                .FilterOn(x => x.Timestamp >= startDate && x.Timestamp <= endDate && x.CarType == carType));

            Check.That(result.Documents).HasSize(2);
        }

        [Fact]
        public void TestSimpleComparisonFilter()
        {
            AddSimpleTestData();
            var startDate = new DateTime(2010, 1, 1);
            var endDate = new DateTime(2010, 3, 1);
            var result = client.Search<Car>(s => s
                .FilterOn(x => x.Timestamp > startDate && x.Timestamp < endDate));
            Check.That(result.Documents).HasSize(1);
        }

        [Fact]
        public void TestEqualityFilter()
        {
            AddSimpleTestData();

            //Standard Nest way of getting the docuements. Values are lowered by ES
            var result = client.Search<Car>(s => s.Filter(x => x.Term(f => f.CarType, "type0")));
            Check.That(result.Documents).HasSize(5);

            //Little bit better
            result = client.Search<Car>(s => s.FilterOn(x => x.CarType, "Type0"));
            Check.That(result.Documents).HasSize(5);


            //Best way
            result = client.Search<Car>(s => s.FilterOn(x => x.CarType == "Type0"));
            Check.That(result.Documents).HasSize(5);

        }

        [Fact]
        public void TestRangeFilters_And_And()
        {
            AddSimpleTestData();

            var startDate = new DateTime(2010, 1, 1);
            var endDate = new DateTime(2010, 5, 1);
         
            var result = client.Search<Car>(s => s.Query(
                    q=>q.Filtered(fil=>fil.Filter(
                        x => x.And(
                            left=>left.Range(f=>f.OnField(fd=>fd.Timestamp).Greater(startDate)),
                            right=>right.Range(f=>f.OnField(fd=>fd.Timestamp).Lower(endDate))
                        )
                    )
                )
            ));
            Check.That(result.Documents).HasSize(3);

            //Much better
            result = client.Search<Car>(s => s.FilteredOn(f => f.Timestamp > startDate && f.Timestamp < endDate));
            Check.That(result.Documents).HasSize(3);
        }
    }
}
