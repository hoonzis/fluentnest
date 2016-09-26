using System;
using System.Linq.Expressions;

namespace FluentNest
{
    public static class Names
    {
        public static string GenerateFilterName(this Expression expression)
        {
            var expType = expression.NodeType;

            if (expType == ExpressionType.AndAlso || expType == ExpressionType.Or || expType == ExpressionType.OrElse || expType == ExpressionType.Equal
                || expType == ExpressionType.LessThan || expType == ExpressionType.GreaterThan || expType == ExpressionType.LessThanOrEqual || expType == ExpressionType.GreaterThanOrEqual
                || expType == ExpressionType.NotEqual)
            {
                var binaryExpression = (BinaryExpression)expression;
                var leftFilterName = GenerateFilterName(binaryExpression.Left);
                var rightFilterName = GenerateFilterName(binaryExpression.Right);
                return leftFilterName + "_" + expType + "_" + rightFilterName;
            }

            if (expType == ExpressionType.MemberAccess)
            {
                var memberExpression = (MemberExpression)expression;
                //here we handle binary expressions in the from .field.hasValue
                if (memberExpression.Member.Name == "HasValue")
                {
                    var parentFieldExpression = (memberExpression.Expression as MemberExpression);
                    var parentFieldName = parentFieldExpression.GetFieldNameFromMember();
                    return parentFieldName + ".hasValue";
                }
                return memberExpression.GetFieldNameFromMember();
            }
            if (expType == ExpressionType.Lambda)
            {
                var lambda = (LambdaExpression)expression;
                return GenerateFilterName(lambda.Body);
            }
            if (expType == ExpressionType.Convert)
            {
                var unary = (UnaryExpression)expression;
                return GenerateFilterName(unary.Operand);
            }
            if (expType == ExpressionType.Constant)
            {
                var constExp = (ConstantExpression)expression;
                return constExp.Value.ToString();
            }

            throw new NotImplementedException();
        }
    }
}
