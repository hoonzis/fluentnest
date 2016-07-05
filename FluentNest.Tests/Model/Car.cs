using System;

namespace FluentNest.Tests.Model
{
    public enum EngineType
    {
        Diesel,
        Standard
    }

    public class Car
    {
        public String Name { get; set; }
        public decimal Price { get; set; }
        public double Length { get; set; }
        public bool Sold { get; set; }
        public DateTime Timestamp { get; set; }
        public String CarType { get; set; }
        public EngineType EngineType { get; set; }
        public decimal? Weight { get; set; } 
        public int? ConditionalRanking { get; set; }
        public decimal? PriceLimit { get; set; }
        public decimal Emissions { get; set; }
        public string Description { get; set; }
        public string Guid { get; set; }
        public long LongField { get; set; }
        public int IntField { get; set; }
    }
}
