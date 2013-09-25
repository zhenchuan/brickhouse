package brickhouse.udf.collect;
/**
 * Copyright 2013 Klout, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **/


/**
 *	 Based on stream-lib
 * 	 https://github.com/clearspring/stream-lib/blob/master/src/main/java/com/clearspring/analytics/stream/StreamSummary.java
 *   return an array with all the values
 */

// import java.util.StreamSummary;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.log4j.Logger;
import com.clearspring.analytics.stream.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaBinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaStringObjectInspector;

@Description(name="topk",
value = "_FUNC_(x, k) - Returns an array of all the elements in the aggregation group " 
)
public class TopKUDAF extends AbstractGenericUDAFResolver {
	private static final Logger LOG = Logger.getLogger(TopKUDAF.class);

	@Override
	public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
			throws SemanticException {
		return new TopKCollectUDAFEvaluator();
	}

	public static class TopKCollectUDAFEvaluator extends GenericUDAFEvaluator {
		// For PARTIAL1 and COMPLETE: ObjectInspectors for original data
		private ObjectInspector inputOI;
		// For PARTIAL2 and FINAL: ObjectInspectors for partial aggregations (list
		// of objs)
		private StandardListObjectInspector loi;
		private BinaryObjectInspector internalMergeOI;
		private static int k;


		static class TopKBuffer implements AggregationBuffer {
			StreamSummary collectArray = new StreamSummary(k);
		}

		public ObjectInspector init(Mode m, ObjectInspector[] parameters)
				throws HiveException {
			super.init(m, parameters);
			// init output object inspectors
			// The output of a partial aggregation is a list
			LOG.info(" TopKUDAF.init() - Mode= " + m.name() );
      		for(int i=0; i<parameters.length; ++i) {
        		LOG.info(" ObjectInspector[ "+ i + " ] = " + parameters[i]);
      		}

			if (m == Mode.PARTIAL1) {
				inputOI = parameters[0];

				if(!( parameters[1] instanceof ConstantObjectInspector) ) {
						throw new HiveException("Top K size must be a constant");
				}

				ConstantObjectInspector constInspector = (ConstantObjectInspector) parameters[1];
				Object kObj = constInspector.getWritableConstantValue();
				LOG.info(" Setting batch size to " + kObj);
				k = Integer.valueOf(kObj.toString());

				return PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector;
				// ObjectInspectorFactory
						// .getStandardListObjectInspector( ObjectInspectorUtils
								// .getStandardObjectInspector(inputOI));
			} else if (m == Mode.PARTIAL2) {
				internalMergeOI = (JavaBinaryObjectInspector) parameters[0];
				return internalMergeOI;

			} else if (m == Mode.FINAL) {
				internalMergeOI = (JavaBinaryObjectInspector) parameters[0];
				return (StandardListObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
			}


	/*
	if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
    	  //// iterate() gets called.. string is passed in
    	  this.inputStrOI = (StringObjectInspector) parameters[0];
      } else { /// Mode m == Mode.PARTIAL2 || m == Mode.FINAL
    	   /// merge() gets called ... map is passed in ..
    	  this.partialBufferOI = (BinaryObjectInspector) parameters[0];
    	  
        		 
      } 
      /// The intermediate result is a map of hashes and strings,
      /// The final result is an array of strings
      if( m == Mode.FINAL || m == Mode.COMPLETE) {
    	  /// for final result
    	  return  PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector;
      } else { /// m == Mode.PARTIAL1 || m == Mode.PARTIAL2 
    	  return  PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector;
      }
    }*/



		}

		@Override
		public AggregationBuffer getNewAggregationBuffer() throws HiveException {
			AggregationBuffer buff= new TopKBuffer();
			reset(buff);
			return buff;
		}

		@Override
		public void iterate(AggregationBuffer agg, Object[] parameters)
				throws HiveException {
			Object p = parameters[0];

			if (p != null) {
				TopKBuffer myagg = (TopKBuffer) agg;
				putIntoSet(p, myagg);
			}
		}

		@Override
		public void merge(AggregationBuffer agg, Object partial)
				throws HiveException {
			TopKBuffer myagg = (TopKBuffer) agg;
			byte[] partialResult = internalMergeOI.getPrimitiveJavaObject(partial);
			StreamSummary ret = new StreamSummary (partialResult);
			for(Object i : partialResult) {
				putIntoSet(i, myagg);
			}
		}

		@Override
		public void reset(AggregationBuffer buff) throws HiveException {
			TopKBuffer arrayBuff = (TopKBuffer) buff;
			arrayBuff.collectArray = new StreamSummary(k);
		}

		@Override
		public Object terminate(AggregationBuffer agg) throws HiveException {
			TopKBuffer myagg = (TopKBuffer) agg;
			StreamSummary<Object> ret = new StreamSummary<Object>(myagg.collectArray.size());
			ret.addAll(myagg.collectArray);
			return ret;

		}

		private void putIntoSet(Object p, TopKBuffer myagg) {
			Object pCopy = ObjectInspectorUtils.copyToStandardObject(p,
					this.inputOI);
			myagg.collectArray.offer( pCopy);
		}

		@Override
		public Object terminatePartial(AggregationBuffer agg) throws HiveException {
/*			TopKBuffer myagg = (TopKBuffer) agg;
			StreamSummary<Object> ret = new StreamSummary<Object>(myagg.collectArray.size());
			ret.addAll(myagg.collectArray);
			return ret;*/

			TopKBuffer myagg = (TopKBuffer) agg;
			return myagg.collectMap.toBytes();
		}
	}

}
