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

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import eu.coherentpaas.cqe.CQEException;
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
import eu.coherentpaas.cqe.plan.TableRef;
import eu.coherentpaas.cqe.plan.Union;
import eu.coherentpaas.cqe.plan.Update;

public class RemoteCallFunctionTablesGenerator implements OperationVisitor {

	private QueryContext ctx;

	private String home;

	private int operationId = 0;

	public RemoteCallFunctionTablesGenerator(QueryContext ctx) {
		this.ctx = ctx;
		this.home = CloudMdSQLManager.getInstance().getLocalHome();
	}

	@Override
	public Operation visit(TableRef op) throws CQEException {
		operationId++;
		op.setId(operationId);
		// it is an internal table reference
		return op;
	}

	@Override
	public Operation visit(Project op) throws CQEException {
		operationId++;
		op.setId(operationId); 
		Operation[] ops = op.getOperands();
		List<Type[]> types = new LinkedList<Type[]>();
		for (Operation innerOp : ops) {
			innerOp.accept(this);
			types.add((Type[]) innerOp.getVisitorResult());
		}

		NamedExpr[] columns = op.getColumns();
		Type[] resultTypes = new Type[columns.length];
		int i = 0;
		for (NamedExpr c : columns) {
			Expression expr = c.getValue();
			ExpressionProcessor visitor = new ExpressionProcessor(types);
			expr.accept(visitor);
			resultTypes[i] = (Type) expr.getVisitorResult();
			i++;
		}
		op.setVisitorResult(resultTypes);
		String home = op.getHome();

		if (home == null) {
			home = "node" + op.getId();
			op.setHome(home);
		} 
		if (!home.equals(this.home)) {
			ctx.generateDerbyFunctionForRemoteOperation(op.getHome(),
					op.getId(), resultTypes);
		}
		return op;
	}

	@Override
	public Operation visit(Select op) throws CQEException {
		operationId++;
		op.setId(operationId);
		List<Type> types = new LinkedList<Type>();
		Operation[] ops = op.getOperands();
		for (Operation innerOp : ops) {
			innerOp.accept(this);
			Type[] tmpResult = (Type[]) innerOp.getVisitorResult();
			if (tmpResult != null) {
				for (Type type : tmpResult) {
					types.add(type);
				}
			}
		}
		Type[] array = new Type[types.size()];
		op.setVisitorResult(types.toArray(array));
		String home = op.getHome();
		if (home == null) {
			home = "node" + op.getId();
			op.setHome(home);
		} 
		if (!home.equals(this.home)) {
			ctx.generateDerbyFunctionForRemoteOperation(op.getHome(),
					op.getId(), array);
		}
		return op;
	}

	@Override
	public Operation visit(Join op) throws CQEException {
		operationId++;
		op.setId(operationId);
		Operation[] ops = op.getOperands();
		List<Type[]> types = new LinkedList<Type[]>();
		for (Operation innerOp : ops) {
			innerOp.accept(this);
			types.add((Type[]) innerOp.getVisitorResult());
		}

		Expression[] expressions = op.getResult();
		Type[] resultTypes = new Type[expressions.length];
		int i = 0;
		for (Expression expr : expressions) {
			ExpressionProcessor visitor = new ExpressionProcessor(types);
			expr.accept(visitor);
			resultTypes[i] = (Type) expr.getVisitorResult();
			i++;
		}
		op.setVisitorResult(resultTypes);

		String home = op.getHome();
		if (home == null) {
			home = "node" + op.getId();
			op.setHome(home);
		} 
		if (!home.equals(this.home)) {
			ctx.generateDerbyFunctionForRemoteOperation(op.getHome(),
					op.getId(), resultTypes);
		}
		return op;
	}

	@Override
	public Operation visit(Aggregate op) throws CQEException {
		operationId++;
		op.setId(operationId);
		List<Type> types = new LinkedList<Type>();
		Operation[] ops = op.getOperands();
		for (Operation innerOp : ops) {
			innerOp.accept(this);
			Type[] tmpResult = (Type[]) innerOp.getVisitorResult();
			if (tmpResult != null) {
				for (Type type : tmpResult) {
					types.add(type);
				}
			}
		}
		Type[] array = new Type[types.size()];
		op.setVisitorResult(types.toArray(array));
		String home = op.getHome();
		if (home == null) {
			home = "node" + op.getId();
			op.setHome(home);
		} 
		if (!home.equals(this.home)) {
			ctx.generateDerbyFunctionForRemoteOperation(op.getHome(),
					op.getId(), array);
		}
		return op;
	}

	@Override
	public Operation visit(Call op) throws CQEException {
		operationId++;
		op.setId(operationId);
		Type[] columns = ctx.getSignature(op.getSub());
		op.setVisitorResult(columns);
		String home = op.getHome();
		if (home == null) {
			home = "node" + op.getId();
			op.setHome(home);
		} 
		if (!home.equals(this.home)) {
			ctx.generateDerbyFunctionForRemoteOperation(op.getHome(),
					op.getId(), columns);
		}
		return op;
	}

	@Override
	public Operation visit(Union op) throws CQEException {
		operationId++;
		op.setId(operationId);
		Operation[] ops = op.getOperands();
		List<Type[]> types = new LinkedList<Type[]>();
		for (Operation innerOp : ops) {
			innerOp.accept(this);
			types.add((Type[]) innerOp.getVisitorResult());
		}

		Expression[] expressions = op.getResult();
		Type[] resultTypes = new Type[expressions.length];
		int i = 0;
		for (Expression expr : expressions) {
			ExpressionProcessor visitor = new ExpressionProcessor(types);
			expr.accept(visitor);
			resultTypes[i] = (Type) expr.getVisitorResult();
			i++;
		}
		op.setVisitorResult(resultTypes);

		String home = op.getHome();
		if (home == null) {
			home = "node" + op.getId();
			op.setHome(home);
		} 
		if (!home.equals(this.home)) {
			ctx.generateDerbyFunctionForRemoteOperation(op.getHome(),
					op.getId(), resultTypes);
		}
		return op;
	}

	private class ExpressionProcessor implements ExpressionVisitor {

		private List<Type[]> types;

		public ExpressionProcessor(List<Type[]> types) {
			this.types = types;
		}

		@Override
		public Expression visit(ColRef expr) throws CQEException {
			String[] refs = expr.getColref();
			Integer tableIndex = 0;
			Integer columnIndex = 0;
			if (refs.length == 2) {
				tableIndex = Integer.parseInt(refs[1]);
			}
			columnIndex = Integer.parseInt(refs[0]);
			expr.setVisitorResult(this.types.get(tableIndex)[columnIndex]);

			return expr;
		}

		@Override
		public Expression visit(Const expr) throws CQEException {
			expr.setVisitorResult(expr.getDatatype());
			return expr;
		}

		@Override
		public Expression visit(Func expr) throws CQEException {
			String function = expr.getFunction();
			Type result = null;
			Function[] numeric = new Function[] { Function.DIVIDE,
					Function.MINUS, Function.ABS, Function.MOD, Function.PLUS,
					Function.TIMES, Function.SQRT };

			boolean isNumeric = false;
			for (int i = 0; i < numeric.length && !isNumeric; i++) {
				isNumeric = numeric[i].getSymbol().equals(function);
			}
			if (isNumeric) {
				// there is a Function to deduce the function type
				Expression[] operands = expr.getOperands();
				List<Type> types = new LinkedList<Type>();

				if (operands != null) {
					for (Expression operand : operands) {
						operand.accept(this);
						Type aux = (Type) operand.getVisitorResult();
						types.add(aux);
					}
				}
				boolean equalsType = true;
				Iterator<Type> it = types.iterator();
				Type old = null;
				while (it.hasNext() && equalsType) {
					Type next = it.next();
					equalsType = old != null && old.equals(next);
					if (!equalsType) {
						if (old.equals(Type.INT) || next.equals(Type.INT)) {
							result = Type.INT;
						} else {
							result = next;
						}
					}
				}

			}
			expr.setVisitorResult(result);
			return expr;
		}

		@Override
		public Expression visit(Param param) throws CQEException {

			return param;
		}

		@Override
		public Expression visit(InList expr) throws CQEException {
			// TODO Auto-generated method stub
			return expr;
		}

		@Override
		public Expression visit(InQuery expr) throws CQEException {
			// TODO Auto-generated method stub
			return expr;
		}

		@Override
		public Expression visit(SetFunc expr) throws CQEException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Expression visit(OperationExpression operationExpression) {
			// TODO Auto-generated method stub
			return null;
		}
	}

	@Override
	public Operation visit(Sort sort) throws CQEException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Operation visit(Limit limit) throws CQEException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Operation visit(Execute execute) throws CQEException {
		// TODO Auto-generated method stub
		return null;
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
