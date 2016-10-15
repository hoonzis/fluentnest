using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using FluentNest.Tests.Model;
using Nest;
using NFluent;
using Tests;
using Xunit;

namespace FluentNest.Tests
{
    public class HistogramTests : TestsBase
    {
        private string AddSimpleTestData()
        {
            var indexName = "index_" + Guid.NewGuid();
            Client.CreateIndex(indexName, x => x.Mappings(m => m.Map<Car>(t => t.Properties(prop => prop.String(str => str.Name(s => s.EngineType).Index(FieldIndexOption.NotAnalyzed))))));
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
                Client.Index(car, ind => ind.Index(indexName));
            }
            Client.Flush(indexName);
            return indexName;
        }

        [Fact]
        public void MonthlyHistogramPerCarType()
        {
            var index = AddSimpleTestData();

            var histogram = Client.Search<Car>(s => s.Index(index).Aggregations(a => a
                .DateHistogram(x => x.Timestamp, DateInterval.Month)
                .GroupBy(x => x.CarType))
            );

            var aggsContainer = histogram.Aggs.AsContainer<Car>();

            var carTypes = aggsContainer.GetDictionary(x => x.CarType, v => v.GetDateHistogram<Car>(f => f.Timestamp));

            Check.That(carTypes).HasSize(3);

            // currently nest returns buckets in between the values
            // first type gets everything between first month and the 10th month -> that is 10 buckets
            var firstType = carTypes["type0"];
            Check.That(firstType).HasSize(10);

            // second type and third type both get 7 buckets
            // second type from february to september
            var secondType = carTypes["type1"];
            Check.That(secondType).HasSize(7);

            // third type from marz to october
            var thirdType = carTypes["type2"];
            Check.That(thirdType).HasSize(7);

            Client.DeleteIndex(index);
        }

        [Fact]
        public void SumInMonthlyHistogram()
        {
            var index = AddSimpleTestData();

            // First the standad NEST way
            var result = Client.Search<Car>(s => s.Index(index).Aggregations(a => a.DateHistogram("by_month",
               d => d.Field(x => x.Timestamp)
                       .Interval(DateInterval.Month)
                       .Aggregations(
                           aggs => aggs.Sum("priceSum", dField => dField.Field(field => field.Price))))));

            var histogram = result.Aggs.DateHistogram("by_month");
            Check.That(histogram.Buckets).HasSize(10);
            var firstMonth = histogram.Buckets[0];
            var priceSum = firstMonth.Sum("priceSum");
            Check.That(priceSum.Value.Value).Equals(10d);


            // Now with FluentNest
            result = Client.Search<Car>(sc => sc.Index(index).Aggregations(agg => agg
                .SumBy(x=>x.Price)
                .IntoDateHistogram(date => date.Timestamp, DateInterval.Month))
            );

            var histogram2 = result.Aggs.GetDateHistogram<Car>(x => x.Timestamp);
            Check.That(histogram2).HasSize(10);
            Check.That(histogram2.All(x => x.GetSum<Car, decimal>(s => s.Price) == 10m)).IsTrue();
        }

        [Fact]
        public void MonthlySumHistogramFilteredOnDates()
        {
            var index = AddSimpleTestData();
            var start = new DateTime(2010, 1, 1);
            var end = new DateTime(2010, 4, 4);

            var result = Client.Search<Car>(sc => sc.Index(index)
                .FilterOn(f => f.Timestamp < end && f.Timestamp > start)
                .Aggregations(agg => agg
                    .SumBy(x => x.Price)
                    .IntoDateHistogram(date => date.Timestamp, DateInterval.Month)
                ));

            var histogram = result.Aggs.GetDateHistogram<Car>(x => x.Timestamp);

            Check.That(histogram).HasSize(3);
            Client.DeleteIndex(index);
        }

        [Fact]
        public void StandardHistogramTest()
        {
            var index = AddSimpleTestData();
            
            var result = Client.Search<Car>(sc => sc.Index(index)
                .Aggregations(x => x
                    .SumBy(y => y.Price)
                    .IntoHistogram(y => y.Length, 5)
                ));

            var histogram = result.Aggs.GetHistogram<Car>(x => x.Length);
            Check.That(histogram).HasSize(4);
            Client.DeleteIndex(index);
        }
    }
}
