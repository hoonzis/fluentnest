﻿using System;
using System.Collections.Generic;

namespace FluentNest.Tests.Model
{
    public enum EngineType
    {
        Diesel,
        Standard
    }

    public class Car
    {
        public Car()
        {
            TyresInstalled = new List<Tyres>();
        }

        public Guid Id { get; set; }
        public String Name { get; set; }
        public decimal Price { get; set; }
        public double Length { get; set; }
        public bool Sold { get; set; }
        public DateTime Timestamp { get; set; }
        public String CarType { get; set; }
        public EngineType EngineType { get; set; }
        public EngineType? NullableEngineType { get; set; }
        public decimal? Weight { get; set; } 
        public int? ConditionalRanking { get; set; }
        public decimal? PriceLimit { get; set; }
        public decimal Emissions { get; set; }
        public string Description { get; set; }
        public string Guid { get; set; }
        public bool Active { get; set; }
        public int Age { get; set; }
        public bool Enabled { get; set; }

        public string BIG_CASE_NAME { get; set; }

        // Of course cars don't have emails, but for my tests it's useful
        public string Email { get; set; }
        public DateTime? LastControlCheck { get; set; }
        public DateTime? LastAccident { get; set; }

        public string[] PreviousOwners { get; set; }

        public List<Tyres> TyresInstalled { get; set; }
    }

    public class Tyres
    {
        public int CompanyId { get; set; }

        public string Company { get; set; }

        public DateTime InstallationDate { get; set; }

        public bool IsPuncture { get; set; }

        public int Age { get; set; }

        public decimal? Weight { get; set; }
    }
}
