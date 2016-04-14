using System;
using System.Collections.Generic;
using System.Linq;
using FluentNest;
using Nest;
using static Nest.Infer;
using NFluent;
using TestModel;
using Xunit;

namespace Tests
{
    public class HistogramTests
    {
        private ElasticClient client;

        public HistogramTests()
        {
            var node = new Uri("http://localhost:9600");

            var settings = new ConnectionSettings(node).DefaultIndex("my-application");

            client = new ElasticClient(settings);
        }

        private void AddSimpleTestData()
        {
            client.DeleteIndex(Index<Car>());
            for (int i = 0; i < 10; i++)
            {
                var car = new Car
                {
                    Timestamp = new DateTime(2010,i+1,1),
                    Name = "Car" + i,
                    Price = 10,
                    Sold = i % 2 == 0 ? true : false,
                    CarType = "Type" + i%3,
                    Length = i*2,
                    Weight = i
                };
                client.Index(car);
            }
            client.Flush(Index<Car>());
        }

        [Fact]
        public void MonthlyHistogramPerCarType()
        {
            AddSimpleTestData();

            var histogram = client.Search<Car>(s => s.Aggregations(a => a
                .DateHistogram(x => x.Timestamp, DateInterval.Month)
                .GroupBy(x => x.CarType))
            );

            var carTypes =
                histogram.Aggs.GetDictionary<Car, IList<DateHistogramItem>>(x => x.CarType,
                    v => v.GetDateHistogram<Car>(f => f.Timestamp));

            Check.That(carTypes).HasSize(3);

            var firstType = carTypes["type0"];
            Check.That(firstType).HasSize(10);
            Check.That(firstType.Sum(x => x.DocCount)).IsEqualTo(4);
        }

        [Fact]
        public void HistogramOfSumsStandardWay()
        {
            AddSimpleTestData();
            var result = client.Search<Car>(s => s.Aggregations(a => a.DateHistogram("by_month",
                d => d.Field(x => x.Timestamp)
                        .Interval(DateInterval.Month)
                        .Aggregations(
                            aggs => aggs.Sum("priceSum", dField => dField.Field(field => field.Price))))));

            var histogram = result.Aggs.DateHistogram("by_month");
            Check.That(histogram.Items).HasSize(10);
            var firstMonth = histogram.Items[0];
            var priceSum = firstMonth.Sum("priceSum");
            Check.That(priceSum.Value.Value).Equals(10d);
        }

        [Fact]
        public void SumInMonthlyHistogram()
        {
            AddSimpleTestData();
            var result = client.Search<Car>(sc => sc.Aggregations(agg => agg
                .SumBy(x=>x.Price)
                .IntoDateHistogram(date => date.Timestamp, DateInterval.Month))
            );

            var histogram = result.Aggs.GetDateHistogram<Car>(x => x.Timestamp);
            Check.That(histogram).HasSize(10);
            Check.That(histogram.All(x => x.GetSum<Car, decimal>(s => s.Price) == 10m)).IsTrue();
        }

        [Fact]
        public void MonthlySumHistogramFilteredOnDates()
        {
            AddSimpleTestData();
            var start = new DateTime(2010, 1, 1);
            var end = new DateTime(2010, 4, 4);

            var result = client.Search<Car>(sc => sc
                .FilteredOn(f => f.Timestamp < end && f.Timestamp > start)
                .Aggregations(agg => agg
                    .SumBy(x => x.Price)
                    .IntoDateHistogram(date => date.Timestamp, DateInterval.Month)
                ));

            var histogram = result.Aggs.GetDateHistogram<Car>(x => x.Timestamp);

            Check.That(histogram).HasSize(3);
        }

        [Fact]
        public void StandardHistogramTest()
        {
            AddSimpleTestData();
            
            var result = client.Search<Car>(sc => sc
                .Aggregations(x => x
                    .SumBy(y => y.Price)
                    .IntoHistogram(y => y.Length, 5)
                ));

            var histogram = result.Aggs.GetHistogram<Car>(x => x.Length);
            Check.That(histogram).HasSize(4);
        }
    }
}
