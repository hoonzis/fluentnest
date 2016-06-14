using System;
using System.Collections.Generic;
using System.Linq;
using FluentNest.Tests.Model;
using Nest;
using NFluent;
using Tests;
using Xunit;

namespace FluentNest.Tests
{
    public class HistogramTests : TestsBase
    {
        private void AddSimpleTestData()
        {
            Client.DeleteIndex(CarIndex);
            Client.CreateIndex(CarIndex, x => x.Mappings(m => m.Map<Car>(t => t.Properties(prop => prop.String(str => str.Name(s => s.EngineType).Index(FieldIndexOption.NotAnalyzed))))));
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
                Client.Index(car);
            }
            Client.Flush(Infer.Index<Car>());
        }

        [Fact]
        public void MonthlyHistogramPerCarType()
        {
            AddSimpleTestData();

            var histogram = Client.Search<Car>(s => s.Aggregations(a => a
                .DateHistogram(x => x.Timestamp, DateInterval.Month)
                .GroupBy(x => x.CarType))
            );

            var carTypes =
                histogram.Aggs.GetDictionary<Car, IList<DateHistogramBucket>>(x => x.CarType,
                    v => v.GetDateHistogram<Car>(f => f.Timestamp));

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
        }

        [Fact]
        public void HistogramOfSumsStandardWay()
        {
            AddSimpleTestData();
            var result = Client.Search<Car>(s => s.Aggregations(a => a.DateHistogram("by_month",
                d => d.Field(x => x.Timestamp)
                        .Interval(DateInterval.Month)
                        .Aggregations(
                            aggs => aggs.Sum("priceSum", dField => dField.Field(field => field.Price))))));

            var histogram = result.Aggs.DateHistogram("by_month");
            Check.That(histogram.Buckets).HasSize(10);
            var firstMonth = histogram.Buckets[0];
            var priceSum = firstMonth.Sum("priceSum");
            Check.That(priceSum.Value.Value).Equals(10d);
        }

        [Fact]
        public void SumInMonthlyHistogram()
        {
            AddSimpleTestData();
            var result = Client.Search<Car>(sc => sc.Aggregations(agg => agg
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

            var result = Client.Search<Car>(sc => sc
                .FilterOn(f => f.Timestamp < end && f.Timestamp > start)
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
            
            var result = Client.Search<Car>(sc => sc
                .Aggregations(x => x
                    .SumBy(y => y.Price)
                    .IntoHistogram(y => y.Length, 5)
                ));

            var histogram = result.Aggs.GetHistogram<Car>(x => x.Length);
            Check.That(histogram).HasSize(4);
        }
    }
}
