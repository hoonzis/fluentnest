
namespace FluentNest
{
    using System.Linq.Expressions;
    public static class ExpressionTypeExtension
    {
        public static bool IsComparisonType(this ExpressionType expType)
        {
            return expType == ExpressionType.LessThan || expType == ExpressionType.GreaterThan ||
                   expType == ExpressionType.LessThanOrEqual || expType == ExpressionType.GreaterThanOrEqual;
        }
    }
}
