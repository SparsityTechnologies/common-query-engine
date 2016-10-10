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

import java.io.IOException;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.ObjectNode;

import eu.coherentpaas.cqe.NativeQueryPlan;
import eu.coherentpaas.cqe.QueryPlan;
import eu.coherentpaas.cqe.TransformQueryPlan;
import eu.coherentpaas.cqe.Type;
import eu.coherentpaas.cqe.QueryPlan.Parameter;
import eu.coherentpaas.cqe.plan.Operation;

public class TableExprDeserializer extends StdDeserializer<TableExpr> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7890506034268836437L;

	protected TableExprDeserializer(Class<?> arg0) {
		super(arg0);

	}

	@Override
	public TableExpr deserialize(JsonParser jp, DeserializationContext ctxt)
			throws IOException, JsonProcessingException {
		ObjectMapper mapper = (ObjectMapper) jp.getCodec();
		ObjectNode root = (ObjectNode) mapper.readTree(jp);
		Class<? extends QueryPlan> planClass = null;
		JsonNode plan = root.get("plan");
		String operation = plan.get("op").textValue();

		if (operation.equals("NATIVE")) {
			planClass = NativeQueryPlan.class;
		} else if (operation.equals("TRANSFORM")) {
			planClass = TransformQueryPlan.class;
		}
		if (planClass != null) {

			QueryPlan qp = mapper.treeToValue(plan, planClass);
			TableExpr expr = new TableExpr();
			expr.setName(root.get("name").textValue());
			if (root.has("scalar")) {
				qp.isScalar(Boolean.valueOf(root.get("scalar").textValue()));
			}
			expr.setPlan(qp);
			return expr;
		} else {
			Operation op = mapper.treeToValue(plan, Operation.class);
			Type[] sig = null;
			boolean scalar = false;
			if (root.has("signature")) {
				sig = mapper.treeToValue(root.get("signature"), Type[].class);
			}
			Parameter[] params = null;
			String[] references = null;
			if (root.has("params")) {
				params = mapper.treeToValue(root.get("params"), Parameter[].class);
			}
			if (root.has("scalar")) {
				scalar = Boolean.valueOf(root.get("scalar").textValue());
			}
			if (root.has("references")) {
				references = mapper.treeToValue(root.get("references"), String[].class);
			}
			TransformQueryPlan qp = new TransformQueryPlan("", sig, params, op, scalar, references);
			TableExpr expr = new TableExpr();
			expr.setName(root.get("name").textValue());
			expr.setPlan(qp);
			return expr;
		}

	}

}
