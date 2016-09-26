using System;
using System.Linq.Expressions;
using Nest;

namespace FluentNest
{
    public static class Ranges
    {
        public static RangeFilterDescriptor<T> RangeOnDate<T>(this RangeFilterDescriptor<T> filterDescriptor, ExpressionType type, DateTime value) where T : class
        {
            if (type == ExpressionType.LessThan)
            {
                return filterDescriptor.Lower(value);
            }
            if (type == ExpressionType.GreaterThan)
            {
                return filterDescriptor.Greater(value);
            }
            if (type == ExpressionType.LessThanOrEqual)
            {
                return filterDescriptor.LowerOrEquals(value);
            }
            if (type == ExpressionType.GreaterThanOrEqual)
            {
                return filterDescriptor.GreaterOrEquals(value);
            }
            throw new NotImplementedException();
        }

        public static RangeFilterDescriptor<T> RangeOnNumber<T>(this RangeFilterDescriptor<T> filterDescriptor, ExpressionType type, double value) where T : class
        {
            if (type == ExpressionType.LessThan)
            {
                return filterDescriptor.Lower(value);
            }
            if (type == ExpressionType.GreaterThan)
            {
                return filterDescriptor.Greater(value);
            }
            if (type == ExpressionType.LessThanOrEqual)
            {
                return filterDescriptor.LowerOrEquals(value);
            }
            if (type == ExpressionType.GreaterThanOrEqual)
            {
                return filterDescriptor.GreaterOrEquals(value);
            }
            throw new NotImplementedException();
        }

        public static FilterContainer GenerateRangeFilter<T>(string fieldName, object leftValue, ExpressionType leftType, object rightValue, ExpressionType rightType)
            where T : class
        {
            var filterDescriptor = new FilterDescriptor<T>();
            if (leftValue is DateTime)
            {
                var leftDate = (DateTime)leftValue;
                var rightDate = (DateTime)rightValue;
                return filterDescriptor.Range(x=> x.RangeOnDate(leftType, leftDate).RangeOnDate(rightType, rightDate).OnField(fieldName));
            }

            if (leftValue is decimal || leftValue is double || leftValue is int || leftValue is long)
            {
                var left = Convert.ToDouble(leftValue);
                var right = Convert.ToDouble(rightValue);
                return filterDescriptor.Range(x => x.RangeOnNumber(leftType, left).RangeOnNumber(rightType, right).OnField(fieldName));
            }

            throw new NotImplementedException();
        }
    }
}