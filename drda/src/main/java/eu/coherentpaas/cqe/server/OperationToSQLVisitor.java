/* Copyright 2016 Sparsity-Technologies
 
 The research leading to this code has been partially funded by the
 European Commission under FP7 programme project #611068.
 
 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at
 
     http://www.apache.org/licenses/LICENSE-2.0
 
 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */
package eu.coherentpaas.cqe.server;

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import javax.swing.SortOrder;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.sql.compile.CompilerContext;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DateTimeDataValue;
import org.apache.derby.iapi.types.SQLBlob;
import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.iapi.types.UserType;
import org.apache.derby.iapi.util.ReuseFactory;
import org.apache.derby.impl.sql.compile.AggregateNode;
import org.apache.derby.impl.sql.compile.AndNode;
import org.apache.derby.impl.sql.compile.BinaryArithmeticOperatorNode;
import org.apache.derby.impl.sql.compile.BinaryRelationalOperatorNode;
import org.apache.derby.impl.sql.compile.BooleanConstantNode;
import org.apache.derby.impl.sql.compile.CastNode;
import org.apache.derby.impl.sql.compile.CharConstantNode;
import org.apache.derby.impl.sql.compile.ColumnReference;
import org.apache.derby.impl.sql.compile.CompilerContextImpl;
import org.apache.derby.impl.sql.compile.ConditionalNode;
import org.apache.derby.impl.sql.compile.CursorNode;
import org.apache.derby.impl.sql.compile.FromList;
import org.apache.derby.impl.sql.compile.FromSubquery;
import org.apache.derby.impl.sql.compile.FromTable;
import org.apache.derby.impl.sql.compile.FromVTI;
import org.apache.derby.impl.sql.compile.GroupByColumn;
import org.apache.derby.impl.sql.compile.GroupByList;
import org.apache.derby.impl.sql.compile.HalfOuterJoinNode;
import org.apache.derby.impl.sql.compile.InListOperatorNode;
import org.apache.derby.impl.sql.compile.IsNullNode;
import org.apache.derby.impl.sql.compile.JavaToSQLValueNode;
import org.apache.derby.impl.sql.compile.JoinNode;
import org.apache.derby.impl.sql.compile.MethodCallNode;
import org.apache.derby.impl.sql.compile.NewInvocationNode;
import org.apache.derby.impl.sql.compile.NonStaticMethodCallNode;
import org.apache.derby.impl.sql.compile.NotNode;
import org.apache.derby.impl.sql.compile.NumericConstantNode;
import org.apache.derby.impl.sql.compile.OrNode;
import org.apache.derby.impl.sql.compile.OrderByColumn;
import org.apache.derby.impl.sql.compile.OrderByList;
import org.apache.derby.impl.sql.compile.ParameterNode;
import org.apache.derby.impl.sql.compile.QueryTreeNode;
import org.apache.derby.impl.sql.compile.ResultColumn;
import org.apache.derby.impl.sql.compile.ResultColumnList;
import org.apache.derby.impl.sql.compile.ResultSetNode;
import org.apache.derby.impl.sql.compile.RowNumberFunctionNode;
import org.apache.derby.impl.sql.compile.SelectNode;
import org.apache.derby.impl.sql.compile.StaticMethodCallNode;
import org.apache.derby.impl.sql.compile.SubqueryNode;
import org.apache.derby.impl.sql.compile.TableName;
import org.apache.derby.impl.sql.compile.TernaryOperatorNode;
import org.apache.derby.impl.sql.compile.UnaryArithmeticOperatorNode;
import org.apache.derby.impl.sql.compile.UnionNode;
import org.apache.derby.impl.sql.compile.UntypedNullConstantNode;
import org.apache.derby.impl.sql.compile.UserTypeConstantNode;
import org.apache.derby.impl.sql.compile.ValueNode;
import org.apache.derby.impl.sql.compile.ValueNodeList;
import org.apache.derby.impl.sql.compile.WindowDefinitionNode;

import eu.coherentpaas.cqe.CQEException;
import eu.coherentpaas.cqe.CQEException.Severity;
import eu.coherentpaas.cqe.QueryPlan.Parameter;
import eu.coherentpaas.cqe.Type;
import eu.coherentpaas.cqe.plan.Aggregate;
import eu.coherentpaas.cqe.plan.Call;
import eu.coherentpaas.cqe.plan.ColRef;
import eu.coherentpaas.cqe.plan.Const;
import eu.coherentpaas.cqe.plan.Create;
import eu.coherentpaas.cqe.plan.Delete;
import eu.coherentpaas.cqe.plan.Drop;
import eu.coherentpaas.cqe.plan.Execute;
import eu.coherentpaas.cqe.plan.Expression;
import eu.coherentpaas.cqe.plan.ExpressionVisitor;
import eu.coherentpaas.cqe.plan.Func;
import eu.coherentpaas.cqe.plan.InList;
import eu.coherentpaas.cqe.plan.InQuery;
import eu.coherentpaas.cqe.plan.Insert;
import eu.coherentpaas.cqe.plan.Join;
import eu.coherentpaas.cqe.plan.Limit;
import eu.coherentpaas.cqe.plan.NamedExpr;
import eu.coherentpaas.cqe.plan.Nested;
import eu.coherentpaas.cqe.plan.Operation;
import eu.coherentpaas.cqe.plan.OperationExpression;
import eu.coherentpaas.cqe.plan.OperationVisitor;
import eu.coherentpaas.cqe.plan.Param;
import eu.coherentpaas.cqe.plan.Project;
import eu.coherentpaas.cqe.plan.Select;
import eu.coherentpaas.cqe.plan.SetFunc;
import eu.coherentpaas.cqe.plan.Sort;
import eu.coherentpaas.cqe.plan.SortColumn;
import eu.coherentpaas.cqe.plan.TableRef;
import eu.coherentpaas.cqe.plan.Union;
import eu.coherentpaas.cqe.plan.Update;

public class OperationToSQLVisitor implements OperationVisitor, ExpressionVisitor {

	protected QueryContext ctx;

	private Stack<ResultColumnList[]> operandResults;

	private Stack<QueryTreeNode> scope;

	protected Stack<FromList> fromList;

	protected Stack<GroupByList> groupBy;

	protected Stack<OrderByList> orderBy;
	
	private Stack<Boolean> useTableStore;

	private SelectNode selectNode = null;

	private int limit = -1;

	private org.apache.derby.iapi.services.context.ContextManager cm;

	private static final int OFFSET_CLAUSE = 0;
	private static final int FETCH_FIRST_CLAUSE = OFFSET_CLAUSE + 1;
	private static final int OFFSET_CLAUSE_COUNT = FETCH_FIRST_CLAUSE + 1;

	private static Map<String, Integer> binaryRelationalOperators = null;

	private static Map<String, Integer> binaryArithmeticOperators = null;

	private static Map<String, Integer> unaryArithmeticOperators = null;

	private CompilerContext compilerContext;

	private ArrayList<ParameterNode> parameterList;

	private ArrayList<DataTypeDescriptor> paramDefaultTypes;

	private String nestedTableExpr;

	static {

		binaryRelationalOperators = new HashMap<String, Integer>();

		binaryRelationalOperators.put("=", BinaryRelationalOperatorNode.K_EQUALS);
		binaryRelationalOperators.put("<>", BinaryRelationalOperatorNode.K_NOT_EQUALS);
		binaryRelationalOperators.put(">=", BinaryRelationalOperatorNode.K_GREATER_EQUALS);
		binaryRelationalOperators.put(">", BinaryRelationalOperatorNode.K_GREATER_THAN);
		binaryRelationalOperators.put("<=", BinaryRelationalOperatorNode.K_LESS_EQUALS);
		binaryRelationalOperators.put("<", BinaryRelationalOperatorNode.K_LESS_THAN);

		binaryArithmeticOperators = new HashMap<String, Integer>();
		binaryArithmeticOperators.put("+", BinaryArithmeticOperatorNode.K_PLUS);
		binaryArithmeticOperators.put("-", BinaryArithmeticOperatorNode.K_MINUS);
		binaryArithmeticOperators.put("/", BinaryArithmeticOperatorNode.K_DIVIDE);
		binaryArithmeticOperators.put("*", BinaryArithmeticOperatorNode.K_TIMES);
		binaryArithmeticOperators.put("%", BinaryArithmeticOperatorNode.K_MOD);

		unaryArithmeticOperators = new HashMap<String, Integer>();
		unaryArithmeticOperators.put("||", UnaryArithmeticOperatorNode.K_ABS);
		unaryArithmeticOperators.put("-", UnaryArithmeticOperatorNode.K_MINUS);
		unaryArithmeticOperators.put("+", UnaryArithmeticOperatorNode.K_PLUS);
		unaryArithmeticOperators.put("SQRT", UnaryArithmeticOperatorNode.K_SQRT);

	}

	public OperationToSQLVisitor(QueryContext ctx, String nestedTableExpr) {
		this.ctx = ctx;
		operandResults = new Stack<ResultColumnList[]>();
		scope = new Stack<QueryTreeNode>();
		fromList = new Stack<FromList>();
		groupBy = new Stack<GroupByList>();
		orderBy = new Stack<OrderByList>();
		useTableStore = new Stack<Boolean>();
		this.nestedTableExpr = nestedTableExpr;
	}

	public OperationToSQLVisitor(QueryContext ctx) {
		this(ctx, null);
	}

	public QueryContext getQueryContext() {
		return ctx;
	}

	final void setCompilerContext(CompilerContext cc) {
		this.compilerContext = cc;
		initUnnamedParameterList();
	}

	@Override
	public Operation visit(TableRef op) throws CQEException {

		QueryTreeNode old = scope.peek();
		if (old instanceof SubqueryNode) {

			SubqueryNode aux = (SubqueryNode) old;
			FromSubquery fromTable = new FromSubquery(aux.getResultSet(), aux.getOrderByList(), aux.getOffset(),
					aux.getFetchFirst(), aux.hasJDBClimitClause(), op.getAlias(), null, null, getContextManager());
			FromList fromL = ((SelectNode) aux.getResultSet()).getFromList();
			fromL.addElement(fromTable);
			op.setVisitorResult(fromTable);

		} else {

			FromList fromList = ((SelectNode) old).getFromList();

			if (fromList == null) {
				fromList = new FromList(true, getContextManager());
				((SelectNode) old).setFromList(fromList);

			}

			FromVTI fromTable = null;
			try {

				List<ValueNode> args = new LinkedList<ValueNode>();
				// afegim el nom de la taula i el tx id.
				if(!useTableStore.isEmpty()){
					ctx.setRequiresTableStore(op.getName());
				}
				CharConstantNode name = new CharConstantNode(op.getName(), getContextManager());
				args.add(name);
				NumericConstantNode txId = new NumericConstantNode(TypeId.BIGINT_ID, ctx.getId(), getContextManager());
				args.add(txId);
				String tableName = "CTXT" + ctx.getId() + "_" + op.getName();
				NewInvocationNode newNode = new NewInvocationNode(tableName, // TableName
						args, true, getContextManager());

				JavaToSQLValueNode node = new JavaToSQLValueNode(newNode, getContextManager());

				fromTable = new FromVTI((MethodCallNode) node.getJavaValueNode(), op.getName(), null, null,
						getContextManager());

			} catch (StandardException e) {
				throw new CQEException(CQEException.Severity.Execution, "Error creating the from statement ", e);
			}

			if (fromTable != null) {
				fromList.addElement(fromTable);
			}

		}
		return op;
	}

	private final CompilerContext getCompilerContext() {
		return compilerContext;
	}

	public void initSelectQuery() {
		if (selectNode == null) {
			selectNode = new SelectNode(getContextManager());
			operandResults.push(new ResultColumnList[1]);
			fromList.push(new FromList(getContextManager()));
			groupBy.push(new GroupByList(getContextManager()));
			scope.push(selectNode);
		}
	}

	public QueryTreeNode finishSelectQuery() throws CQEException {
		ResultColumnList[] peek = operandResults.peek();
		ResultColumnList list = peek[0];
		// list.addResultColumn(new AllResultColumn(null, getContextManager()));
		operandResults.pop();

		selectNode.setResultColumns(list);
		scope.pop();
		if (selectNode.getFromList() == null) {
			if (!fromList.isEmpty()) {
				selectNode.setFromList(fromList.pop());
			}
		}
		if (selectNode.getGroupBy() == null) {
			if (!groupBy.isEmpty()) {
				GroupByList groupByList = groupBy.pop();
				if (groupByList.size() > 0) {
					selectNode.setGroupByList(groupByList);
				}
			}
		}
		if (scope.size() == 0) {
			QueryTreeNode current = null;
			try {
				selectNode = new SelectNode(selectNode.getResultColumns(), selectNode.getFromList(),
						selectNode.getWhereClause(), selectNode.getGroupBy(), selectNode.getHavingClause(),
						selectNode.getWindow(), selectNode.getOverwritingPlan(), getContextManager());

				current = generateCursor("SELECT", selectNode, null);

			} catch (StandardException e) {
				throw new CQEException(CQEException.Severity.Execution, "Error creating the Select object ", e);
			}
			return current;
		} else {

			return selectNode;
		}
	}

	protected org.apache.derby.iapi.services.context.ContextManager getContextManager() {
		cm = ContextService.getFactory().getCurrentContextManager();
		if (cm == null) {
			cm = ctx.getCloudMdSQLStatement().getLanguageConnectionContext().getContextManager();
			ContextService.getFactory().setCurrentContextManager(cm);
			cm.pushContext(ctx.getCloudMdSQLStatement().getLanguageConnectionContext());
			// Mirar GenericLanguageConnectionFactory
		}
		return cm;
	}

	@Override
	public Operation visit(Project op) throws CQEException {
		SelectNode selectNode = null;
		QueryTreeNode current = null;
		OrderByList orderByList = null;
		selectNode = new SelectNode(getContextManager());
		Operation[] ops = op.getOperands();
		if (scope.isEmpty()) {

			this.selectNode = selectNode;
			current = selectNode;
		} else {
			if (!(scope.peek() instanceof UnionNode)) {
				SubqueryNode aux = new SubqueryNode(getContextManager());
				current = aux;

				((SubqueryNode) current).setResultSet(selectNode);
				op.setVisitorResult(aux);
			} else {
				op.setVisitorResult(selectNode);
				current = selectNode;
			}
		}

		scope.push(current);

		if (ops != null) {
			orderByList = new OrderByList(selectNode, getContextManager());
			orderBy.push(orderByList);
			operandResults.push(new ResultColumnList[ops.length]);
			fromList.push(new FromList(getContextManager()));
			groupBy.push(new GroupByList(getContextManager()));
			for (Operation innerOp : ops) {
				innerOp.accept(this);

			}
			NamedExpr[] selected = op.getColumns();
			ResultColumnList list = new ResultColumnList(getContextManager());

			/*
			 * INFO: Each result resolves which column corresponds to the colref
			 * expressions, constant values and aggregates
			 */
			if (selected != null) {

				for (int i = 0; i < selected.length; i++) {
					NamedExpr c = selected[i];
					Expression value = c.getValue();
					value.accept(this);
					ValueNode result = (ValueNode) value.getVisitorResult();
					ResultColumn rc = toResultColumn(c.getName(), result);

					if (rc != null) {
						list.addResultColumn(rc);
					}

				}

			}
			ResultColumnList[] genColumns = operandResults.pop();

			selectNode.setResultColumns(list);
			scope.pop();
			if (selectNode.getFromList() == null) {
				if (!fromList.isEmpty()) {
					selectNode.setFromList(fromList.pop());
				}
			}

			if (selectNode.getGroupBy() == null) {
				if (!groupBy.isEmpty()) {
					GroupByList group = groupBy.pop();
					if (group.size() > 0) {
						selectNode.setGroupByList(group);
					}
				}
			}
			if (!orderBy.isEmpty()) {
				orderByList = orderBy.pop();
			}

			if (scope.size() == 0 || (scope.peek() instanceof UnionNode)) {

				try {
					boolean isDistinct = op.isDistinct();
					selectNode = new SelectNode(selectNode.getResultColumns(), selectNode.getFromList(),
							selectNode.getWhereClause(), selectNode.getGroupBy(), selectNode.getHavingClause(),
							selectNode.getWindow(), selectNode.getOverwritingPlan(), getContextManager());
					if (isDistinct) {
						selectNode.makeDistinct();
						CompilerContext cc = getCompilerContext();
						cc.setReliability(cc.getReliability() | CompilerContext.NEXT_VALUE_FOR_ILLEGAL);
					}

					orderByList.setResultSetNode(selectNode);
					if (scope.isEmpty() || !(scope.peek() instanceof UnionNode)) {
						current = generateCursor("SELECT", selectNode, orderByList);
					}
					op.setVisitorResult(current);
					if (!scope.isEmpty() && scope.peek() instanceof UnionNode) {
						addResultColumnList(list);
					}

				} catch (StandardException e) {
					throw new CQEException(null, "Error creating the Select object ", e);
				}
			} else {

				SubqueryNode aux = (SubqueryNode) op.getVisitorResult();

				if (orderByList.isEmpty()) {
					orderByList = null;
				}

				if (op.getOperands().length == 1 && (list.equals(genColumns[0]))
						&& (op.getOperands()[0]) instanceof Call) {
					op.setVisitorResult(op.getOperands()[0].getVisitorResult());
					addResultColumnList(genColumns[0]);
					FromList fromL = fromList.peek();
					FromList selectFrom = selectNode.getFromList();
					for (int i = 0; i < selectFrom.size(); i++) {
						fromL.addElement(selectFrom.elementAt(i));
					}
				} else {
					FromSubquery fromTable = new FromSubquery(aux.getResultSet(), orderByList, aux.getOffset(),
							aux.getFetchFirst(), aux.hasJDBClimitClause(), ctx.generateSubqueryName(), list, null,
							getContextManager());
					if (!fromList.isEmpty()) {
						// fromList.pop();
						FromList fromL = fromList.peek();
						fromL.addElement(fromTable);
					}
					op.setVisitorResult(fromTable);
					int max = list.size();
					ResultColumnList rsl = new ResultColumnList(getContextManager());
					TableName tName = new TableName(
							this.ctx.getCloudMdSQLStatement().getLanguageConnectionContext().getCurrentSchemaName(),
							fromTable.getCorrelationName(), getContextManager());

					try {
						for (int i = 0; i < max; i++) {
							ResultColumn rc = list.elementAt(i);
							ColumnReference cref = new ColumnReference(rc.getName(), tName, getContextManager());

							ResultColumn rcolumn;

							rcolumn = new ResultColumn(cref, cref, getContextManager());

							rsl.addResultColumn(rcolumn);

						}
						addResultColumnList(rsl);
					} catch (StandardException e) {
						throw new RuntimeException(e);
					}
				}

			}

		}

		return op;

	}

	protected void addResultColumnList(ResultColumnList resultList) {
		ResultColumnList[] peek = operandResults.peek();
		boolean inserted = false;
		for (int i = 0; i < peek.length && !inserted; i++) {
			if (peek[i] == null) {
				peek[i] = resultList;
				inserted = true;
			}
		}
	}

	@Override
	public Operation visit(Select op) throws CQEException {

		ResultColumnList[] list = null;
		QueryTreeNode select = scope.peek();
		Operation[] ops = op.getOperands();
		boolean representsHaving = false;
		boolean pushed = false;
		if (ops != null) {

			list = new ResultColumnList[ops.length];
			operandResults.push(list);
			representsHaving = (ops.length == 1 && (ops[0] instanceof Aggregate));
			if (!representsHaving) {
				pushed = true;
				fromList.push(new FromList(getContextManager()));
				groupBy.push(new GroupByList(getContextManager()));
			}
			for (Operation operand : ops) {
				operand.accept(this);
			}
		}
		Expression filter = op.getFilter();
		filter.accept(this);

		if (ops != null) {
			list = operandResults.pop();
		}
		if (select instanceof SelectNode) {
			SelectNode selectNode = (SelectNode) select;
			if (representsHaving) {
				selectNode.setFromList(fromList.pop());
				GroupByList groupByList = groupBy.pop();

				if (groupByList.size() > 0) {
					selectNode.setGroupByList(groupByList);
					selectNode.setHavingClause((ValueNode) filter.getVisitorResult());
				}
			} else {
				selectNode.setWhereClause((ValueNode) filter.getVisitorResult());
			}
			addResultColumnList(list[0]);
			op.setVisitorResult(select);
		} else {
			SubqueryNode subquery = (SubqueryNode) select;
			select = subquery.getResultSet();
			SelectNode selectNode = (SelectNode) select;
			if (representsHaving) {

				selectNode.setFromList(fromList.pop());
				GroupByList groupByList = groupBy.pop();
				if (groupByList.size() > 0) {
					selectNode.setGroupByList(groupByList);
					selectNode.setHavingClause((ValueNode) filter.getVisitorResult());
				}
			} else {
				if (pushed) {
					selectNode.setFromList(fromList.pop());
					GroupByList groupByList = groupBy.pop();
					if (groupByList.size() > 0) {
						selectNode.setGroupByList(groupByList);
						selectNode.setHavingClause((ValueNode) filter.getVisitorResult());
					}
				}
				selectNode.setWhereClause((ValueNode) filter.getVisitorResult());
			}
			op.setVisitorResult(subquery);
			addResultColumnList(list[0]);
		}

		return op;
	}

	@Override
	public Operation visit(Join op) throws CQEException {

		JoinNode node = null;
		Operation[] ops = op.getOperands();
		if (ops != null) {
			useTableStore.push(true);
			fromList.push(new FromList(getContextManager()));
			ResultColumnList[] results = new ResultColumnList[ops.length];
			operandResults.push(results);

			Operation left = ops[0];
			Operation right = ops[1];
			fromList.push(new FromList(getContextManager()));
			left.accept(this);

			right.accept(this);

			Expression condition = op.getCondition();
			condition.accept(this);
			fromList.pop();
			// Join node

			Object leftResult = left.getVisitorResult();

			Object rightResult = right.getVisitorResult();

			try {
				if (op.getType().equals("INNER")) {
					node = new JoinNode((org.apache.derby.impl.sql.compile.ResultSetNode) leftResult,
							(org.apache.derby.impl.sql.compile.ResultSetNode) rightResult,
							(ValueNode) condition.getVisitorResult(), null, null, null, null, getContextManager());
					node.setNaturalJoin();
				} else if (op.getType().equals("LEFT")) {
					node = new HalfOuterJoinNode((org.apache.derby.impl.sql.compile.ResultSetNode) leftResult,
							(org.apache.derby.impl.sql.compile.ResultSetNode) rightResult,
							(ValueNode) condition.getVisitorResult(), null, false, null, getContextManager());
				} else if (op.getType().equals("RIGHT")) {
					node = new HalfOuterJoinNode((org.apache.derby.impl.sql.compile.ResultSetNode) leftResult,
							(org.apache.derby.impl.sql.compile.ResultSetNode) rightResult,
							(ValueNode) condition.getVisitorResult(), null, true, null, getContextManager());
				}

			} catch (StandardException e) {
				throw new CQEException(null, "Error creating the Join object ", e);
			}
			Expression[] result = op.getResult();
			ResultColumnList list = new ResultColumnList(getContextManager());
			if (result != null) {

				for (int i = 0; i < result.length; i++) {
					result[i].accept(this);
					if (result[i] instanceof ColRef) {

						ResultColumn original = (ResultColumn) result[i].getVisitorResult();
						// original.setResultSetNumber(resultSetNumber);
						list.addResultColumn(original);

					}

				}
			}
			fromList.pop();
			operandResults.pop();
			useTableStore.pop();
			addResultColumnList(list);

			FromList fromL = fromList.peek();

			fromL.addElement(node);

			op.setVisitorResult(node);
		}

		return op;
	}

	private ResultColumn toResultColumn(String name, ValueNode result) throws CQEException {
		ResultColumn rc = null;
		if (result != null) {
			try {
				if (result instanceof ResultColumn) {
					rc = (ResultColumn) result;
					if (name != null && !name.trim().equals("")) {
						rc = new ResultColumn((String) name, rc, getContextManager());
					}

				} else {

					if (name != null && name.trim().equals("")) {
						name = null;
					}
					rc = new ResultColumn((String) name, result, getContextManager());

				}
			} catch (StandardException e) {
				throw new CQEException(CQEException.Severity.Execution,
						"Error creating a result column object from an expression", e);
			}
		}
		return rc;
	}

	@Override
	public Operation visit(Aggregate aggregate) throws CQEException {

		ResultColumnList list = new ResultColumnList(getContextManager());
		addResultColumnList(list);

		Operation[] ops = aggregate.getOperands();
		if (ops != null) {

			ResultColumnList[] opsList = new ResultColumnList[ops.length];
			operandResults.push(opsList);
			for (Operation op : ops) {
				op.accept(this);
			}

			// the tables are in the stack
			NamedExpr[] groupByColumns = aggregate.getGroupBy();
			ResultColumnList[] resultColumnList = new ResultColumnList[1];
			resultColumnList[0] = new ResultColumnList(getContextManager());
			if (groupByColumns != null) {
				for (NamedExpr arg : groupByColumns) {
					Expression value = arg.getValue();
					value.accept(this);
					ResultColumn rc = toResultColumn(arg.getName(), (ValueNode) value.getVisitorResult());
					if (rc != null) {
						// rc.markAsGroupingColumn();
						resultColumnList[0].addResultColumn(rc);
						list.addResultColumn(rc);
					}
				}
			}

			GroupByList groupByList = groupBy.peek();
			Iterator<ResultColumn> it = resultColumnList[0].iterator();
			while (it.hasNext()) {
				ResultColumn rc = it.next();
				try {
					if (rc.getReference() != null) {
						groupByList.addElement(new GroupByColumn(rc.getReference().getClone(), getContextManager()));
					} else if (rc.getExpression() != null) {
						groupByList.addElement(new GroupByColumn(rc.getExpression(), getContextManager()));
					}
				} catch (StandardException e) {
					throw new CQEException(CQEException.Severity.Execution,
							"Error creating the group by list for an aggregate", e);
				}

			}

			NamedExpr[] aggregates = aggregate.getAggregates();
			if (aggregates != null) {

				for (NamedExpr aggr : aggregates) {
					Expression value = aggr.getValue();
					value.accept(this);
					ResultColumn rc;
					try {
						rc = new ResultColumn((String) null, (ValueNode) value.getVisitorResult(), getContextManager());
						list.addResultColumn(rc);
					} catch (StandardException e) {
						throw new CQEException(CQEException.Severity.Execution,
								"Error creating the result column of an aggregate", e);
					}

				}

			}
			operandResults.pop();

		}

		return aggregate;
	}

	public void addParamType(eu.coherentpaas.cqe.Type type) throws Exception {
		if (type.equals(Type.STRING)) {
			paramDefaultTypes.add(new DataTypeDescriptor(TypeId.VARCHAR_ID, true));
		} else if (type.equals(Type.INT)) {
			paramDefaultTypes.add(new DataTypeDescriptor(TypeId.INTEGER_ID, true));
		} else if (type.equals(Type.LONG)) {
			paramDefaultTypes.add(new DataTypeDescriptor(TypeId.BIGINT_ID, true));
		} else if (type.equals(Type.DOUBLE)) {
			paramDefaultTypes.add(new DataTypeDescriptor(TypeId.DOUBLE_ID, true));
		} else if (type.equals(Type.FLOAT)) {
			paramDefaultTypes.add(new DataTypeDescriptor(TypeId.REAL_ID, true));
		} else if (type.equals(Type.TIMESTAMP)) {
			paramDefaultTypes.add(new DataTypeDescriptor(TypeId.TIMESTAMP_ID, true));
		}else if (type.equals(Type.DATE)) {
			paramDefaultTypes.add(new DataTypeDescriptor(TypeId.DATE_ID, true));
		} else if (type.equals(Type.BOOL)) {
			paramDefaultTypes.add(new DataTypeDescriptor(TypeId.BOOLEAN_ID, true));
		} else if (type.equals(Type.ARRAY) || type.equals(Type.DICTIONARY)) {
			if(type.equals(Type.ARRAY)){
				paramDefaultTypes.add(new DataTypeDescriptor(
						TypeId.getUserDefinedTypeId(ctx.getSchema(), type.toSQLType(), CloudMdSQLArray.class.getName()), true));
			}
			else{
				paramDefaultTypes.add(new DataTypeDescriptor(
						TypeId.getUserDefinedTypeId(ctx.getSchema(), type.toSQLType(), CloudMdSQLMap.class.getName()), true));
			}
			
		} else if (type.equals(Type.BINARY)) {
			paramDefaultTypes.add(new DataTypeDescriptor(TypeId.BLOB_ID, true));
		} else if (type.equals(Type.UNKNOWN)) {
			TypeId typeId = TypeId.getUserDefinedTypeId(Type.UNKNOWN.name());
			if (typeId != null) {
				paramDefaultTypes.add(new DataTypeDescriptor(typeId, true));
			}
		} else {
			throw new Exception("Invalid type: " + type.name());
		}
	}

	private JavaToSQLValueNode getCall(Call call, Expression expr) throws CQEException {
		String table = "CTXT" + ctx.getId() + "_" + call.getSub().toUpperCase();
		JavaToSQLValueNode javaNode = null;

		List<ValueNode> parameters = new LinkedList<ValueNode>();
		try {

			if(!useTableStore.isEmpty()){
				ctx.setRequiresTableStore(call.getSub());
			}
			ValueNode param1 = new CharConstantNode(call.getSub(), getContextManager());
			ValueNode param2 = new NumericConstantNode(TypeId.BIGINT_ID, ctx.getId(), getContextManager());

			parameters.add(param1);
			parameters.add(param2);

			if (expr != null && expr instanceof Const) {
				Const indexValueExpr = (Const) expr;
				String value = indexValueExpr.getValue().toString();
				Integer pos = Integer.valueOf(value);
				table += "_" + pos;
				ValueNode param = new NumericConstantNode(TypeId.INTEGER_ID, pos, getContextManager());
				parameters.add(param);
			}

			NamedExpr[] args = call.getParams();

			Parameter[] params = ctx.getTableParameters(call.getSub());

			if (args != null) {
				int i = 0;
				for (NamedExpr arg : args) {

					Expression value = arg.getValue();
					if (nestedTableExpr == null) {
						if (value instanceof Param) {
							 addParamType(params[i].getType());
						}
					}
					value.accept(this);
					parameters.add((ValueNode) value.getVisitorResult());
					i++;
				}

			}

			TableName tName = new TableName(
					ctx.getCloudMdSQLStatement().getLanguageConnectionContext().getCurrentSchemaName(), table,
					getContextManager());

			MethodCallNode callNode = new StaticMethodCallNode(tName, null, getContextManager());
			callNode.addParms(parameters);
			javaNode = new JavaToSQLValueNode(callNode, getContextManager());

		} catch (Exception e) {
			throw new CQEException(CQEException.Severity.Execution, "Error creating the VTI object of a call", e);
		}

		return javaNode;

	}

	@Override
	public Operation visit(Call call) throws CQEException {

		ResultColumnList list = new ResultColumnList(getContextManager());

		Map<String, Integer> columns = ctx.getColumnIndexes(call.getSub());

		Set<String> names = columns.keySet();

		Iterator<String> it = names.iterator();

		try {

			JavaToSQLValueNode javaNode = getCall(call, null);
			StaticMethodCallNode method = (StaticMethodCallNode) javaNode.getJavaValueNode();
			while (it.hasNext()) {
				String next = it.next();

				ColumnReference cref = new ColumnReference("R" + next, method.getFullName(), getContextManager());
				// DefaultNode value = new DefaultNode();
				ResultColumn column;
				// int i = 1;
				try {
					column = new ResultColumn(cref, cref, getContextManager());

					// column.setResultSetNumber(resultSetNumber);
					list.addResultColumn(column);
					// i++;

				} catch (StandardException e) {
					throw new CQEException(null, "Error creating the columns of a call", e);
				}

			}
			FromVTI result = new FromVTI((MethodCallNode) javaNode.getJavaValueNode(),
					method.getFullName().getTableName(), list, null, getContextManager());

			FromList fromL = fromList.peek();
			fromL.addElement(result);

			call.setVisitorResult(result);

		} catch (Exception e) {
			throw new CQEException(CQEException.Severity.Execution, "Error creating the VTI object of a call", e);
		}

		addResultColumnList(list);

		return call;
	}

	@Override
	public Expression visit(ColRef expr) throws CQEException {

		ResultColumnList[] list = operandResults.peek();
		String[] refs = expr.getColref();
		Integer tableIndex = 0;
		if (refs.length == 2) {
			tableIndex = Integer.parseInt(refs[1]);
		}

		ResultColumnList operandResult = list[tableIndex];

		Integer columnIndex = Integer.parseInt(refs[0]);

		ResultColumn rc = operandResult.elementAt(columnIndex);
		try {
			ResultColumn rcolumn = null;
			ColumnReference cref = rc.getReference();
			if (cref != null) {
				cref = (ColumnReference) cref.getClone();
				rcolumn = new ResultColumn(cref, cref, getContextManager());
			} else if (rc.getExpression() != null) {
				rcolumn = rc;
			}
			expr.setVisitorResult(rcolumn);

		} catch (StandardException e) {
			throw new CQEException(CQEException.Severity.Execution, "Error creating the const object", e);
		}
		return expr;
	}

	private ValueNode getJdbcIntervalNode(String val) throws StandardException {
		int intervalType = 0;
		if (val.equals("minute")) {
			intervalType = DateTimeDataValue.MINUTE_INTERVAL;
		} else if (val.equals("second")) {
			intervalType = DateTimeDataValue.SECOND_INTERVAL;
		} else if (val.equals("hour")) {
			intervalType = DateTimeDataValue.HOUR_INTERVAL;
		} else if (val.equals("month")) {
			intervalType = DateTimeDataValue.MONTH_INTERVAL;
		} else if (val.equals("day")) {
			intervalType = DateTimeDataValue.DAY_INTERVAL;
		} else if (val.equals("year")) {
			intervalType = DateTimeDataValue.YEAR_INTERVAL;
		} else if (val.equals("week")) {
			intervalType = DateTimeDataValue.WEEK_INTERVAL;
		}

		return new NumericConstantNode(TypeId.getBuiltInTypeId(Types.INTEGER), ReuseFactory.getInteger(intervalType),
				getContextManager());
	}

	@Override
	public Expression visit(Const expr) throws CQEException {
		Type type = expr.getDatatype();
		ValueNode value = null;
		try {
			if (Type.STRING == type) {

				value = new CharConstantNode(expr.getValue().toString(), getContextManager());
			} else if (Type.INT == type) {

				value = new NumericConstantNode(TypeId.INTEGER_ID, Integer.parseInt(expr.getValue().toString()),
						getContextManager());
			} else if (Type.TIMESTAMP == type) {

				value = new UserTypeConstantNode((java.sql.Timestamp) expr.getValue(), getContextManager());
			}else if (Type.DATE == type) {
				value = new UserTypeConstantNode((java.sql.Date) expr.getValue(), getContextManager());
				
			} else if (Type.FLOAT == type) {

				value = new NumericConstantNode(TypeId.REAL_ID, Float.parseFloat(expr.getValue().toString()),
						getContextManager());
			} else if (Type.BINARY == type) {

				SQLBlob myObject = new SQLBlob((byte[]) expr.getValue());
				value = new UserTypeConstantNode(myObject, getContextManager());
			} else if (Type.ARRAY == type || Type.DICTIONARY == type) {

				UserType myObject = new UserType(expr.getValue());
				value = new UserTypeConstantNode(myObject, getContextManager());
			} else if (Type.DOUBLE == type) {
				value = new NumericConstantNode(TypeId.DOUBLE_ID, Double.parseDouble(expr.getValue().toString()),
						getContextManager());
			} else if (Type.BOOL == type) {

				value = new BooleanConstantNode(Boolean.parseBoolean(expr.getValue().toString()), getContextManager());
			} else {
				throw new CQEException(null, "Invalid const value", null);
			}
		} catch (StandardException e) {
			throw new CQEException(CQEException.Severity.Execution, "Error creating the const object", e);
		}
		expr.setVisitorResult(value);
		return expr;
	}

	@Override
	public Expression visit(Func expr) throws CQEException {
		Expression[] ops = expr.getOperands();
		ValueNode result = null;
		if (!expr.getFunction().equals("@ARRAY")) {
			if (ops != null) {
				for (Expression op : ops) {
					op.accept(this);
					Object opRes = op.getVisitorResult();
					if (opRes instanceof ResultColumn) {
						ResultColumn resCol = (ResultColumn) opRes;
						Object opResult = null;
						if (resCol.getReference() != null) {
							opResult = resCol.getReference();
						} else if (resCol.getExpression() != null) {
							opResult = resCol.getExpression();
						} else {
							throw new CQEException(CQEException.Severity.Execution, "Invalid result column", null);
						}
						op.setVisitorResult(opResult);
					}
				}
			}
		}
		try {
			if (binaryRelationalOperators.containsKey(expr.getFunction())) {
				result = new BinaryRelationalOperatorNode(binaryRelationalOperators.get(expr.getFunction()),
						(ValueNode) ops[0].getVisitorResult(), (ValueNode) ops[1].getVisitorResult(), false,
						getContextManager());
			} else if (binaryArithmeticOperators.containsKey(expr.getFunction()) && ops.length == 2) {
				result = new BinaryArithmeticOperatorNode(binaryArithmeticOperators.get(expr.getFunction()),
						(ValueNode) ops[0].getVisitorResult(), (ValueNode) ops[1].getVisitorResult(),
						getContextManager());
			} else if (unaryArithmeticOperators.containsKey(expr.getFunction()) && ops.length == 1) {
				result = new UnaryArithmeticOperatorNode((ValueNode) ops[0].getVisitorResult(),
						unaryArithmeticOperators.get(expr.getFunction()), getContextManager());
			} else if (expr.getFunction().equals("#DICTIONARY")) {

				List<ValueNode> params = new LinkedList<ValueNode>();
				ValueNode previous = null;
				for (int i = 0; i < ops.length; i++) {
					ValueNode aux = (ValueNode) ops[i].getVisitorResult();
					if (i % 2 == 1) {
						List<ValueNode> paramsAux = new LinkedList<ValueNode>();
						paramsAux.add(previous);
						paramsAux.add(aux);
						NewInvocationNode nin = new NewInvocationNode(CloudMdSQLMapEntry.class.getName(), paramsAux,
								false, getContextManager());
						params.add(new JavaToSQLValueNode(nin, getContextManager()));
					} else {
						previous = aux;
					}
				}

				NewInvocationNode nin = new NewInvocationNode(CloudMdSQLMap.class.getName(), params, false,
						getContextManager());

				result = new JavaToSQLValueNode(nin, getContextManager());

			} else if (expr.getFunction().equals("@DICTIONARY")) {
				ValueNode implicitObject = (ValueNode) ops[0].getVisitorResult();

				if (implicitObject instanceof JavaToSQLValueNode) {
					TypeId typeId = TypeId.getUserDefinedTypeId(CloudMdSQLMap.class.getName());
					implicitObject = new CastNode(implicitObject, new DataTypeDescriptor(typeId, true),
							getContextManager());
				}
				NonStaticMethodCallNode node = new NonStaticMethodCallNode("get", implicitObject, getContextManager());
				List<ValueNode> params = new LinkedList<ValueNode>();
				params.add((ValueNode) ops[1].getVisitorResult());
				node.addParms(params);
				result = new JavaToSQLValueNode(node, getContextManager());

			} else if (expr.getFunction().equals("#ARRAY")) {

				List<ValueNode> params = new LinkedList<ValueNode>();

				for (int i = 0; i < ops.length; i++) {
					params.add((ValueNode) ops[i].getVisitorResult());
				}

				NewInvocationNode nin = new NewInvocationNode(CloudMdSQLArray.class.getName(), params, false,
						getContextManager());

				result = new JavaToSQLValueNode(nin, getContextManager());

			} else if (expr.getFunction().equals("@ARRAY")) {
				Expression[] innerOps = expr.getOperands();
				if (innerOps[0] instanceof OperationExpression) {
					Operation aux = ((OperationExpression) innerOps[0]).getValue();

					if (aux instanceof Call) {
						Call call = (Call) aux;
						result = getCall(call, expr.getOperands()[1]);
					}
				} else {

					for (Expression op : ops) {
						op.accept(this);
						Object opRes = op.getVisitorResult();
						if (opRes instanceof ResultColumn) {
							ResultColumn resCol = (ResultColumn) opRes;
							Object opResult = null;
							if (resCol.getReference() != null) {
								opResult = resCol.getReference();
							} else if (resCol.getExpression() != null) {
								opResult = resCol.getExpression();
							} else {
								throw new CQEException(CQEException.Severity.Execution, "Invalid result column", null);
							}
							op.setVisitorResult(opResult);
						}
					}
					ValueNode implicitObject = (ValueNode) ops[0].getVisitorResult();
					if (implicitObject instanceof JavaToSQLValueNode || implicitObject instanceof ParameterNode) {
						TypeId typeId = TypeId.getUserDefinedTypeId(CloudMdSQLArray.class.getName());
						implicitObject = new CastNode(implicitObject, new DataTypeDescriptor(typeId, true),
								getContextManager());
					}

					NonStaticMethodCallNode node = new NonStaticMethodCallNode("get", implicitObject,
							getContextManager());
					List<ValueNode> params = new LinkedList<ValueNode>();
					params.add((ValueNode) ops[1].getVisitorResult());
					node.addParms(params);
					result = new JavaToSQLValueNode(node, getContextManager());
				}

			} else if (expr.getFunction().equals("AND")) {
				result = new AndNode((ValueNode) ops[0].getVisitorResult(), (ValueNode) ops[1].getVisitorResult(),
						getContextManager());
			} else if (expr.getFunction().equals("OR")) {
				result = new OrNode((ValueNode) ops[0].getVisitorResult(), (ValueNode) ops[1].getVisitorResult(),
						getContextManager());
			} else if (expr.getFunction().equals("XOR")) {
				result = new OrNode(
						new AndNode(new NotNode((ValueNode) ops[0].getVisitorResult(), getContextManager()),
								(ValueNode) ops[1].getVisitorResult(), getContextManager()),
						new AndNode((ValueNode) ops[0].getVisitorResult(),
								new NotNode((ValueNode) ops[1].getVisitorResult(), getContextManager()),
								getContextManager()),
						getContextManager());
			} else if (expr.getFunction().equals("NOT")) {
				result = new NotNode((ValueNode) ops[0].getVisitorResult(), getContextManager());
			} else if (expr.getFunction().equals("IS NULL")) {
				result = new IsNullNode((ValueNode) ops[0].getVisitorResult(), false, getContextManager());
			} else if (expr.getFunction().equals("row_number")) {
				result = new RowNumberFunctionNode(null, new WindowDefinitionNode(null, null, getContextManager()),
						getContextManager());
			} else if (expr.getFunction().equals("timestampadd")) {
				Const cons = (Const) ops[0];
				String val = cons.getValue().toString();
				ValueNode dataTime = getJdbcIntervalNode(val);
				result = new TernaryOperatorNode((ValueNode) ops[2].getVisitorResult(), dataTime,
						(ValueNode) ops[1].getVisitorResult(), TernaryOperatorNode.K_TIMESTAMPADD, -1,
						getContextManager());

			} else if (expr.getFunction().equals("CASE")) {
				ContextManager cm = getContextManager();
				ValueNodeList whenList = new ValueNodeList(cm);
				ValueNodeList thenElseList = new ValueNodeList(cm);
				ValueNode ref = (ValueNode) ops[0].getVisitorResult();

				if (ref instanceof ColumnReference) {

					for (int i = 1; i < ops.length; i++) {
						ValueNode val = (ValueNode) ops[i].getVisitorResult();
						if (i % 2 == 0) {
							thenElseList.addElement(val);
						} else {
							if (i + 1 < ops.length) {
								val = new BinaryRelationalOperatorNode(binaryRelationalOperators.get("="), ref, val,
										false, getContextManager());
								whenList.addElement(val);
							} else {
								thenElseList.addElement(val);
							}
						}
					}
					if (ops.length % 2 == 1) {
						// ELSE NULL is implicit if there is no ELSE clause.
						thenElseList.addElement(new UntypedNullConstantNode(cm));
					}
				} else {
					whenList.addElement((ValueNode) ops[1].getVisitorResult());
					for (int i = 2; i < ops.length; i++) {
						ValueNode val = (ValueNode) ops[i].getVisitorResult();
						thenElseList.addElement(val);
					}
				}

				result = new ConditionalNode(null, whenList, thenElseList, cm);
			} else {
				String aggrName = expr.getFunction();
				if (aggrName.equals("count") && ops == null) {
					aggrName = "count( * )";
				}

				AggregatorKind aggr = AggregatorKind.getAggregatorByCloudMdSQLName(aggrName);
				if (aggr != null) {
					ValueNode node = null;
					if (ops != null && ops.length > 0) {
						node = (ValueNode) ops[0].getVisitorResult();
					}
					result = new AggregateNode(node, aggr.getFunctionClass(), false, aggr.getDerbyName(),
							getContextManager());
				}

			}

		} catch (StandardException e) {
			throw new CQEException(CQEException.Severity.Execution, "Error creating the function object", e);
		}
		expr.setVisitorResult(result);
		return expr;
	}

	public ArrayList<ParameterNode> getParameterList() {
		return parameterList;
	}

	public ArrayList<DataTypeDescriptor> getDataValueDescriptors() {
		return paramDefaultTypes;
	}

	/**
	 * Initializes the list of unnamed parameters, i.e., "?" parameters
	 *
	 * Usually, this routine just gets an empty list for the unnamed parameters.
	 *
	 *
	 */
	void initUnnamedParameterList() {
		parameterList = new ArrayList<ParameterNode>();
		paramDefaultTypes = new ArrayList<DataTypeDescriptor>();
	}

	@Override
	public Expression visit(Param param) throws CQEException {

		String name = param.getName();
		try {
			int parameterNumber = 0;

			if (nestedTableExpr != null && ctx.isNestedTable(nestedTableExpr)) {
				parameterNumber = ctx.createParameterId(nestedTableExpr, name);
				TableExpr table = ctx.getTableExpr(nestedTableExpr);
				Parameter[] params = table.getPlan().getParameters();
				for (int i = 0; i < params.length; i++) {
					if (params[i].getName().equals(name)) {
						try {
							addParamType(params[i].getType());
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				}
			} else {
				parameterNumber = Integer.parseInt(name);
			}

			ParameterNode parm = new ParameterNode(parameterNumber - 1,
					paramDefaultTypes.get(paramDefaultTypes.size() - 1).getNull(), getContextManager());

			parameterList.add(parm);

			((CompilerContextImpl) getCompilerContext()).setParameterList(parameterList);

			param.setVisitorResult(parm);

		} catch (StandardException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return param;
	}

	public String getSQL() {

		return "";
	}

	public SelectNode getSelectNode() {
		return selectNode;
	}

	private CursorNode generateCursor(String name, ResultSetNode node, OrderByList orderCols) throws StandardException {
		if (orderCols != null && orderCols.isEmpty()) {
			orderCols = null;
		}
		ValueNode[] offsetClauses = new ValueNode[OFFSET_CLAUSE_COUNT];

		boolean hasJDBClimitClause = limit != -1;
		if (hasJDBClimitClause) {
			offsetClauses[FETCH_FIRST_CLAUSE] = new NumericConstantNode(TypeId.INTEGER_ID, limit, getContextManager());
		}
		int forUpdateState = CursorNode.UNSPECIFIED;
		ArrayList<String> updateColumns = new ArrayList<String>();
		CursorNode cursor = new CursorNode(name, node, null, orderCols, offsetClauses[OFFSET_CLAUSE],
				offsetClauses[FETCH_FIRST_CLAUSE], hasJDBClimitClause, forUpdateState,
				forUpdateState == CursorNode.READ_ONLY ? null : updateColumns.toArray(new String[updateColumns.size()]),
				false, getContextManager());
		return cursor;
	}

	@Override
	public Operation visit(Union union) throws CQEException {

		try {
			QueryTreeNode ctx = null;
			if (!scope.isEmpty()) {
				ctx = scope.peek();
			}

			UnionNode node = new UnionNode(null, null, !union.isDistinct(), false, null, getContextManager());
			scope.push(node);

			Operation[] ops = union.getOperands();
			ResultColumnList[] results = new ResultColumnList[ops.length];
			if (ops != null) {

				operandResults.push(results);
				ops[0].accept(this);

				ops[1].accept(this);

				operandResults.pop();

			}

			node = new UnionNode((org.apache.derby.impl.sql.compile.ResultSetNode) ops[0].getVisitorResult(),
					(org.apache.derby.impl.sql.compile.ResultSetNode) ops[1].getVisitorResult(), !union.isDistinct(),
					false, null, getContextManager());
			union.setVisitorResult(node);
			scope.pop();
			if (scope.isEmpty()) {
				union.setVisitorResult(generateCursor("UNION", node, null));
			}
			if (ctx instanceof SelectNode) {
				String subQueryName = this.ctx.generateSubqueryName();
				FromTable fromTable = new FromSubquery(node, null, null, null, false, subQueryName, null, null,
						getContextManager());

				int max = results[0].size();
				ResultColumnList result = new ResultColumnList(getContextManager());
				for (int i = 0; i < max; i++) {
					ResultColumn rc = results[0].elementAt(i);

					TableName tName = new TableName(
							this.ctx.getCloudMdSQLStatement().getLanguageConnectionContext().getCurrentSchemaName(),
							subQueryName, getContextManager());

					ColumnReference cref = new ColumnReference(rc.getName(), tName, getContextManager());
					ResultColumn rcolumn = new ResultColumn(cref, cref, getContextManager());
					result.addResultColumn(rcolumn);

				}
				addResultColumnList(result);

				fromList.peek().addElement(fromTable);

			}
		} catch (StandardException e) {
			throw new CQEException(null, "Error creating the union object", e);
		}

		return null;
	}

	@Override
	public Expression visit(InList expr) throws CQEException {
		ValueNodeList inList = new ValueNodeList(getContextManager());
		Expression[] operands = expr.getOperands();
		if (operands.length == 1) {
			operands[0].accept(this);
			ValueNode leftOperand = (ValueNode) operands[0].getVisitorResult();
			Expression[] values = expr.getValues();

			for (int i = 0; i < values.length; i++) {
				values[i].accept(this);
				inList.addElement((ValueNode) values[i].getVisitorResult());
			}

			try {
				InListOperatorNode result = new InListOperatorNode(leftOperand, inList, getContextManager());
				expr.setVisitorResult(result);
			} catch (StandardException e) {
				throw new CQEException(Severity.Execution, "Error in InList op", e);
			}

		}
		return expr;
	}

	@Override
	public Expression visit(InQuery inQuery) throws CQEException {

		Operation op = inQuery.getValues();
		// if (op instanceof Call) {

		try {

			ResultColumnList[] previous = operandResults.peek();
			ResultColumnList[] newRCL = new ResultColumnList[previous.length + 1];
			for (int i = 0; i < previous.length; i++) {
				newRCL[i] = previous[i];
			}

			operandResults.push(newRCL);

			Expression[] operands = inQuery.getExpressions();
			for (Expression operand : operands) {
				operand.accept(this);
			}
			if (op instanceof Call) {
				fromList.push(new FromList(getContextManager()));
			}
			op.accept(this);

			SelectNode select = new SelectNode(operandResults.pop()[previous.length], fromList.pop(), null, null, null,
					null, null, getContextManager());

			SubqueryNode subqueryNode = new SubqueryNode(select, SubqueryNode.IN_SUBQUERY,
					(ValueNode) operands[0].getVisitorResult(), null, null, null, false, getContextManager());

			inQuery.setVisitorResult(subqueryNode);

		} catch (Exception e) {
			throw new CQEException(CQEException.Severity.Execution, "Error creating the VTI object of a call", e);
		}
		// }
		return inQuery;
	}

	@Override
	public Expression visit(SetFunc expr) throws CQEException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Operation visit(Sort sort) throws CQEException {

		OrderByList orderByList = orderBy.peek();
		sort.setVisitorResult(orderByList);

		Operation[] operations = sort.getOperands();
		if (operations != null) {

			for (Operation op : operations) {
				op.accept(this);
			}

			SortColumn[] sortColumns = sort.getColumns();
			if (sortColumns != null) {

				for (SortColumn column : sortColumns) {
					Expression expr = column.getColumn();

					expr.accept(this);

					ValueNode original = (ValueNode) expr.getVisitorResult();

					OrderByColumn orderColumn = new OrderByColumn(original, getContextManager());
					if (column.getOrder().equals(SortOrder.DESCENDING)) {
						orderColumn.setDescending();
					}
					orderByList.addOrderByColumn(orderColumn);

				}
			}
		}

		return sort;
	}

	@Override
	public Operation visit(Limit limit) throws CQEException {

		this.limit = limit.getLimit();
		Operation[] operations = limit.getOperands();
		if (operations != null) {
			for (Operation op : operations) {
				op.accept(this);
				limit.setVisitorResult(op.getVisitorResult());
			}
		}

		return limit;
	}

	@Override
	public Operation visit(Execute execute) throws CQEException {

		Operation[] ops = execute.getOperands();

		Operation tree = null;

		for (int i = 0; i < ops.length; i++) {

			Project project = new Project(execute.getHome(),
					new NamedExpr[] { new NamedExpr("", new Const(Type.INT, 0)) }, false, new Operation[] { ops[i] });
			Project root = new Project(project.getHome(), new NamedExpr[] { new NamedExpr("", new Const(Type.INT, 0)) },
					false, new Operation[] { project });
			if (tree == null) {
				tree = root;
			} else {
				Operation[] newOps = new Operation[] { tree, root };
				Union union = new Union(execute.getHome(), newOps, new Expression[] {}, false);
				tree = union;
			}
		}

		tree.accept(this);
		execute.setVisitorResult(tree.getVisitorResult());

		return execute;
	}

	private void prepareCall(Call call) throws CQEException {

		String table = "CTXT" + ctx.getId() + "_" + call.getSub().toUpperCase();

		List<ValueNode> parameters = new LinkedList<ValueNode>();
		try {

			ValueNode param1 = new CharConstantNode(call.getSub(), getContextManager());
			ValueNode param2 = new NumericConstantNode(TypeId.BIGINT_ID, ctx.getId(), getContextManager());

			parameters.add(param1);
			parameters.add(param2);

			NamedExpr[] args = call.getParams();

			Parameter[] params = ctx.getTableParameters(call.getSub());

			if (args != null) {
				int i = 0;
				for (NamedExpr arg : args) {

					Expression value = arg.getValue();
					if (nestedTableExpr == null) {
						if (value instanceof Param) {
							addParamType(params[i].getType());
						}
					}
					value.accept(this);
					parameters.add((ValueNode) value.getVisitorResult());
					i++;
				}

			}

			TableName tName = new TableName(
					ctx.getCloudMdSQLStatement().getLanguageConnectionContext().getCurrentSchemaName(), table,
					getContextManager());

			MethodCallNode callNode = new StaticMethodCallNode(tName, null, getContextManager());
			callNode.addParms(parameters);

			JavaToSQLValueNode javaNode = new JavaToSQLValueNode(callNode, getContextManager());
			call.setVisitorResult(javaNode);

		} catch (Exception e) {
			throw new CQEException(CQEException.Severity.Execution, "Error creating the VTI object of a call", e);
		}

	}

	@Override
	public Expression visit(OperationExpression operationExpression) throws CQEException {
		Call call = (Call) operationExpression.getValue();

		prepareCall(call);
		operationExpression.setVisitorResult(call.getVisitorResult());
		return operationExpression;

	}

	@Override
	public Operation visit(Insert insert) throws CQEException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Operation visit(Update update) throws CQEException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Operation visit(Delete delete) throws CQEException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Operation visit(Create create) throws CQEException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Operation visit(Drop drop) throws CQEException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Operation visit(Nested nested) throws CQEException {
		// TODO Auto-generated method stub
		return null;
	}

}
