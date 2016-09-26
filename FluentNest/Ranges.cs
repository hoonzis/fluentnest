using System;
using System.Linq.Expressions;
using Nest;

namespace FluentNest
{
    public static class Ranges
    {
        public static DateRangeQueryDescriptor<T> RangeOnDate<T>(this DateRangeQueryDescriptor<T> filterDescriptor, ExpressionType type, DateTime value) where T : class
        {
            if (type == ExpressionType.LessThan)
            {
                return filterDescriptor.LessThan(value);
            }
            if (type == ExpressionType.GreaterThan)
            {
                return filterDescriptor.GreaterThan(value);
            }
            if (type == ExpressionType.LessThanOrEqual)
            {
                return filterDescriptor.LessThanOrEquals(value);
            }
            if (type == ExpressionType.GreaterThanOrEqual)
            {
                return filterDescriptor.GreaterThanOrEquals(value);
            }
            throw new NotImplementedException();
        }

        public static NumericRangeQueryDescriptor<T> RangeOnNumber<T>(this NumericRangeQueryDescriptor<T> filterDescriptor, ExpressionType type, double value) where T : class
        {
            if (type == ExpressionType.LessThan)
            {
                return filterDescriptor.LessThan(value);
            }
            if (type == ExpressionType.GreaterThan)
            {
                return filterDescriptor.GreaterThan(value);
            }
            if (type == ExpressionType.LessThanOrEqual)
            {
                return filterDescriptor.LessThanOrEquals(value);
            }
            if (type == ExpressionType.GreaterThanOrEqual)
            {
                return filterDescriptor.GreaterThanOrEquals(value);
            }
            throw new NotImplementedException();
        }

        public static QueryContainer GenerateRangeFilter<T>(string fieldName, object leftValue, ExpressionType leftType, object rightValue, ExpressionType rightType)
            where T : class
        {
            if (leftValue is DateTime)
            {
                var leftDate = (DateTime)leftValue;
                var rightDate = (DateTime)rightValue;
                var filterDescriptor = new QueryContainerDescriptor<T>();
                return filterDescriptor.DateRange(x => x.RangeOnDate(leftType, leftDate).RangeOnDate(rightType, rightDate).Field(fieldName));
            }

            if (leftValue is decimal || leftValue is double || leftValue is long || leftValue is int)
            {
                var left = Convert.ToDouble(leftValue);
                var right = Convert.ToDouble(rightValue);
                var filterDescriptor = new QueryContainerDescriptor<T>();
                return filterDescriptor.Range(x => x.RangeOnNumber(leftType, left).RangeOnNumber(rightType, right).Field(fieldName));
            }

            throw new NotImplementedException();
        }
    }
}
