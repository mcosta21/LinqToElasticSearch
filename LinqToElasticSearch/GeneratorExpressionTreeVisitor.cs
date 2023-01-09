﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Elasticsearch.Net;
using Nest;
using Remotion.Linq.Clauses.Expressions;
using Remotion.Linq.Clauses.ResultOperators;
using Remotion.Linq.Parsing;

namespace LinqToElasticSearch
{
    public class GeneratorExpressionTreeVisitor<T>: ThrowingExpressionVisitor
    {
        private List<QueryContainer> _queryContainers = new List<QueryContainer>();
        private List<QueryContainer> _tempQueryContainers = new List<QueryContainer>();
        private readonly PropertyNameInferrerParser _propertyNameInferrerParser;
        private bool Not { get; set; }
        private string PropertyName { get; set; }
        private object Value { get; set; }
        private ExpressionType? NodeType { get; set; }
        private Type PropertyType { get; set; }
        private bool IsSubTerm { get; set; } = false;


        public GeneratorExpressionTreeVisitor(PropertyNameInferrerParser propertyNameInferrerParser)
        {
            _propertyNameInferrerParser = propertyNameInferrerParser;
        }

        public List<QueryContainer> GetNestExpression(Expression linqExpression)
        {
            Visit(linqExpression);
            
            var qc = new BoolQuery
            {
                Should = ShouldList
            }; 
            
            _queryContainers.Add(qc);
            return _queryContainers.ToList();
        }

        protected override Expression VisitBinary(BinaryExpression expression)
        {
            
            //var expression = ConvertExpressionWithParantheses(binaryExpression);
            var isSubTerm = CheckIfSubTermExpression(expression);

            NodeType = expression.NodeType;
            Visit(expression.Left);
            Visit(expression.Right);

            if (Value == null)
            {
                if (expression.NodeType == ExpressionType.Equal)
                {
                    _tempQueryContainers.Add(new BoolQuery()
                    {
                        MustNot = new QueryContainer[]
                        {
                            new ExistsQuery()
                            {
                                Field = PropertyName
                            }
                        }
                    });
                }
                if (expression.NodeType == ExpressionType.NotEqual)
                {
                    _tempQueryContainers.Add(new BoolQuery()
                    {
                        Must = new QueryContainer[]
                        {
                            new ExistsQuery()
                            {
                                Field = PropertyName
                            }
                        }
                    });
                }
            }
            
            if (PropertyType.IsEnum)
            {
                VisitEnumProperty(expression.NodeType, _tempQueryContainers);
            }
            else
            {
                switch (Value)
                {
                    case DateTime _:
                        VisitDateProperty(expression.NodeType);
                        break;
                    case bool _:
                        var query = VisitBoolProperty(expression.NodeType);
                        if (isSubTerm)
                        {
                            _tempQueryContainers.Add(query);
                        }
                        break;
                    case int _:
                    case long _:
                    case float _:
                    case double _:
                    case decimal _:
                        VisitNumericProperty(expression.NodeType);
                        break;
                    case string _:
                    case Guid _:
                        VisitStringProperty(expression.NodeType);
                        break;
                }
            }

            if (isSubTerm == false && _tempQueryContainers.Any())
            {
                _queryContainers.Add(new BoolQuery
                {
                    Must = _tempQueryContainers
                });
                _tempQueryContainers = new List<QueryContainer>();
            }
            return expression;
        }

        private void VisitStringProperty(ExpressionType expressionType)
        {
            if (Value is Guid valueGuid)
            {
                Value = valueGuid.ToString();
            }
            
            
            if (expressionType == ExpressionType.Equal)
            {
                _queryContainers.Add(new MatchPhraseQuery()
                {
                    Field = $"{PropertyName}",
                    Name = PropertyName,
                    Query = (string) Value
                });
            }
            
            if (expressionType == ExpressionType.NotEqual)
            {
                _queryContainers.Add(new BoolQuery()
                {
                    MustNot =new QueryContainer[]{ new MatchPhraseQuery()
                    {
                        Field = $"{PropertyName}",
                        Name = PropertyName,
                        Query = (string) Value
                    }}
                } );
            }

            if (expressionType == ExpressionType.OrElse)
            {
                ShouldList.AddRange(_queryContainers);
                _queryContainers.Clear();
            }
        }

        public List<QueryContainer> ShouldList = new List<QueryContainer>();

        private IQuery VisitNumericProperty(ExpressionType expressionType)
        {
            double.TryParse(Value.ToString(), out var doubleValue);
           
            switch (expressionType)
            {
                case ExpressionType.Equal:
                    return new BoolQuery
                    {
                        Must = new QueryContainer[]
                        {
                            new TermQuery
                            {
                                Field = PropertyName,
                                Name = PropertyName,
                                Value = doubleValue
                            }
                        }
                    };
                
                case ExpressionType.NotEqual:
                    return new BoolQuery
                    {
                        MustNot = new QueryContainer[]
                        {
                            new TermQuery
                            {
                                Field = PropertyName,
                                Name = PropertyName,
                                Value = doubleValue
                            }
                        }
                    };
                
                case ExpressionType.GreaterThan:
                    return new BoolQuery
                    {
                        Must = new QueryContainer[]
                        {
                            new NumericRangeQuery
                            {
                                Field = PropertyName,
                                Name = PropertyName,
                                GreaterThan = doubleValue
                            }
                        }
                    };
                
                case ExpressionType.GreaterThanOrEqual:
                    return new BoolQuery
                    {
                        Must = new QueryContainer[]
                        {
                            new NumericRangeQuery
                            {
                                Field = PropertyName,
                                Name = PropertyName,
                                GreaterThanOrEqualTo = doubleValue
                            }
                        }
                    };
                
                case ExpressionType.LessThan:
                    return new BoolQuery
                    {
                        Must = new QueryContainer[]
                        {
                            new NumericRangeQuery
                            {
                                Field = PropertyName,
                                Name = PropertyName,
                                LessThan = doubleValue
                            }
                        }
                    };
                
                case ExpressionType.LessThanOrEqual:
                    return new BoolQuery
                    {
                        Must = new QueryContainer[]
                        {
                            new NumericRangeQuery
                            {
                                Field = PropertyName,
                                Name = PropertyName,
                                LessThanOrEqualTo = doubleValue
                            }
                        }
                    };
                case ExpressionType.OrElse:
                    var qc = (new BoolQuery()
                    {
                        Should = new[]{ _queryContainers[0], _queryContainers[1]}
                    }); 
                    _queryContainers.Clear();
                    _queryContainers.Add(qc);
                    break;
            }

            return null;
        }
        
        private void VisitEnumProperty(ExpressionType expressionType, List<QueryContainer> _tempQueryContainers)
        {
            switch (expressionType)
            {
                case ExpressionType.Equal:
                    _tempQueryContainers.Add(new TermQuery()
                    {
                        Field = PropertyName,
                        Name = PropertyName,
                        Value = ConvertEnumValue(typeof(T),PropertyName,Value)
                    });
                    break;
                
                case ExpressionType.NotEqual:
                    _tempQueryContainers.Add(new BoolQuery()
                    {
                        MustNot = new QueryContainer[]{new TermQuery()
                        {
                            Field = PropertyName,
                            Name = PropertyName,
                            Value = ConvertEnumValue(typeof(T), PropertyName, Value)
                        }}
                    });
                    break;
            }
        }

        private QueryContainer VisitBoolProperty(ExpressionType expressionNodeType)
        {
            if (Value is bool boolValue)
                switch (expressionNodeType)
                {
                    case ExpressionType.Equal:
                        return new BoolQuery
                        {
                            Must = new QueryContainer[]
                            {
                                new TermQuery
                                {
                                    Field = PropertyName,
                                    Name = PropertyName,
                                    Value = boolValue
                                }
                            }
                        };
                    case ExpressionType.NotEqual:
                    case ExpressionType.Not:
                        return new BoolQuery
                        {
                            Must = new QueryContainer[]
                            {
                                new TermQuery
                                {
                                    Field = PropertyName,
                                    Name = PropertyName,
                                    Value = !boolValue
                                }
                            }
                        };
                    case ExpressionType.OrElse:
                        var qc = new BoolQuery
                        {
                            Should = new[]{ _queryContainers[0], _queryContainers[1]}
                        }; 
                        _queryContainers.Clear();
                        _queryContainers.Add(qc);
                        break;
                }

            return null;
        }

        private void VisitDateProperty(ExpressionType expressionNodeType)
        {
            if (Value is DateTime dateTime)
                switch (expressionNodeType)
                {
                    case ExpressionType.GreaterThan:
                        _queryContainers.Add(new DateRangeQuery()
                        {
                            Field = PropertyName,
                            Name = PropertyName,
                            GreaterThan = dateTime
                        });
                        break;
                    case ExpressionType.GreaterThanOrEqual:
                        _queryContainers.Add(new DateRangeQuery()
                        {
                            Field = PropertyName,
                            Name = PropertyName,
                            GreaterThanOrEqualTo = dateTime
                        });
                        break;
                    case ExpressionType.LessThan:
                        _queryContainers.Add(new DateRangeQuery()
                        {
                            Field = PropertyName,
                            Name = PropertyName,
                            LessThan = dateTime
                        });
                        break;
                    case ExpressionType.LessThanOrEqual:
                        _queryContainers.Add(new DateRangeQuery()
                        {
                            Field = PropertyName,
                            Name = PropertyName,
                            LessThanOrEqualTo = dateTime
                        });
                        break;
                    case ExpressionType.Equal:
                        _queryContainers.Add(new DateRangeQuery()
                        {
                            Field = PropertyName,
                            Name = PropertyName,
                            GreaterThanOrEqualTo = dateTime,
                            LessThanOrEqualTo = dateTime 
                        });
                        break;
                    case ExpressionType.NotEqual:
                        _queryContainers.Add(new BoolQuery()
                        {
                            MustNot =new QueryContainer[]{ new DateRangeQuery()
                            {
                                Field = PropertyName,
                                Name = PropertyName,
                                GreaterThanOrEqualTo = dateTime,
                                LessThanOrEqualTo = dateTime 
                            }}
                        });
                        break;
                    case ExpressionType.OrElse:
                        var dateContainers =  _queryContainers.Cast<IQueryContainer>().Where(x => x.Range != null && x.Range.Name == PropertyName).Cast<QueryContainer>().ToList();
                        var qc = new BoolQuery
                        {
                            Name = PropertyName,
                            Should = new[]{ dateContainers[0], dateContainers[1]}
                        };
                        
                        _queryContainers.Remove(dateContainers[0]);
                        _queryContainers.Remove(dateContainers[1]);
                        _queryContainers.Add(qc);
                        break;
                } 
        }

        protected override Expression VisitMethodCall(MethodCallExpression expression)
        {
            // In production code, handle this via method lookup tables.
            QueryContainer query;
            switch (expression.Method.Name)
            {
                case "ToLower":
                    Visit(expression.Object);
                    break;
                case "Contains":
                    Visit(expression.Object);
                    Visit(expression.Arguments[0]);
                    var tokens = ((string) Value).Split(' ');
                    if (tokens.Length == 1)
                    {
                        query = (new QueryStringQuery()
                        {
                            Fields=  new[]{ PropertyName },
                            Name = PropertyName,
                            Query = "*" + Value + "*"
                        });
                    }
                    else
                    {
                        query = (new MultiMatchQuery()
                        {
                            Fields=  new[]{ PropertyName },
                            Name = PropertyName,
                            Type = TextQueryType.PhrasePrefix,
                            Query = (string) Value,
                            MaxExpansions = 200
                        });
                    }

                    AddQueryContainer(query);
                    break;
                case "StartsWith":
                    Visit(expression.Object);
                    Visit(expression.Arguments[0]);
                    query = (new QueryStringQuery()
                    {
                        Fields=  new[]{ PropertyName },
                        Name = PropertyName,
                        Query = Value + "*"
                    });
                    AddQueryContainer(query);
                    break;
                case "EndsWith":
                    Visit(expression.Object);
                    Visit(expression.Arguments[0]);
                    query = (new QueryStringQuery()
                    {
                        Fields=  new[]{ PropertyName },
                        Name = PropertyName,
                        Query = "*" + Value
                    });
                    AddQueryContainer(query);
                    break;
                default:
                    return base.VisitMethodCall(expression); // throws
            }
            
            return expression;
        }

        protected override Expression VisitSubQuery(SubQueryExpression expression)
        {
            foreach (var resultOperator in expression.QueryModel.ResultOperators)
            {
                switch (resultOperator)
                {
                    case ContainsResultOperator containsResultOperator:
                        Visit(containsResultOperator.Item);
                        Visit(expression.QueryModel.MainFromClause.FromExpression);

                        switch (containsResultOperator.Item.Type)
                        {
                            case Type guidType when guidType == typeof(Guid):
                                AddQueryContainer(new TermsQuery()
                                {
                                    Field = PropertyName,
                                    Name = PropertyName,
                                    IsVerbatim = true,
                                    Terms = ((IEnumerable<Guid>) Value).Select(x => x.ToString()).ToList()
                                });
                                break;
                            case Type nullableGuidType when nullableGuidType == typeof(Guid?):
                                AddQueryContainer(new TermsQuery()
                                {
                                    Field = PropertyName,
                                    Name = PropertyName,
                                    IsVerbatim = true,
                                    Terms = ((IEnumerable<Guid?>) Value).Select(x => x.ToString())
                                });
                                break;
                        }
                        break;
                }
            }

            return expression;
        }
        private void AddQueryContainer(QueryContainer query)
        {
            if (query != null) 
                if (Not)
                {
                    _tempQueryContainers.Add(new BoolQuery()
                    {
                        MustNot = new[]{query}
                    });
                    Not = false;
                }
                else
                {
                    _tempQueryContainers.Add(query);
                }
        }

        protected override Expression VisitUnary(UnaryExpression expression)
        {
            switch (expression.NodeType)
            {
                case ExpressionType.Not:
                    Not = true;
                    Value = false;
                    break;
            }
            Visit(expression.Operand);
            
            return expression;
        }


        protected override Expression VisitConstant(ConstantExpression expression)
        {
            Value = expression.Value;
            return expression;
        }
        private object ConvertEnumValue(Type entityType, string propertyName,object value)
        {
            var enumValue = Enum.Parse(PropertyType, value.ToString());
            var prop = entityType.GetProperties().FirstOrDefault(x => x.Name.ToLower() == propertyName.ToLower());

            
            if (prop!=null && prop.GetCustomAttributes(true)
                .Any(attribute => attribute is StringEnumAttribute && prop.PropertyType.IsEnum))
            {
                return enumValue.ToString();
            }
            return (int) enumValue;
        }
        
        protected override Expression VisitQuerySourceReference(QuerySourceReferenceExpression expression)
        {
            return expression;
        }
        
        protected override Expression VisitMember(MemberExpression expression)
        {
            Visit(expression.Expression);
            PropertyName = _propertyNameInferrerParser.Parser(expression.Member.Name);
            PropertyType = expression.Type;

            if (expression.Type == typeof(bool))
            {
                if (!(Value is bool))
                {
                    Value = true;
                }

                if (NodeType == null)
                {
                    VisitBoolProperty(ExpressionType.Equal);
                }
            }
            
            return expression;
        }


        protected override Exception CreateUnhandledItemException<T>(T unhandledItem, string visitMethod)
        {
            // string itemText = FormatUnhandledItem(unhandledItem);
            var itemText = "";
            var message = string.Format("The expression '{0}' (type: {1}) is not supported by this LINQ provider.",
                itemText, typeof(T));
            return new NotSupportedException(message);
        }

        private BinaryExpression ConvertExpressionWithParantheses(BinaryExpression expression)
        {
            var expressionChars = expression.ToString().ToCharArray();
            var hasParantheses = expressionChars[0] == '(' && expressionChars[expressionChars.Length - 1] == ')' ;

            if (hasParantheses && Value == null && NodeType == ExpressionType.OrElse)
            {
                
                return BinaryExpression.Equal(expression, Expression.Constant(true));
            }

            return expression;
        }
        
        private bool CheckIfSubTermExpression(BinaryExpression expression)
        {
            var expressionChars = expression.ToString().ToCharArray();
            var hasParantheses = expressionChars[0] == '(' && expressionChars[expressionChars.Length - 1] == ')' ;

            return hasParantheses && Value == null && expression.IsLifted;
        }
    }
}