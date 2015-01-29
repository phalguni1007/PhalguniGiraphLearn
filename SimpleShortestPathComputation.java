/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.rii.giraph.learn;

import org.apache.giraph.Algorithm;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.conf.LongConfOption;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Demonstrates the basic Pregel shortest paths implementation.
 */
@Algorithm(
    name = "Shortest paths",
    description = "Finds all shortest paths from a selected vertex"
)
public class SimpleShortestPathComputation extends BasicComputation<
    LongWritable, Text, Text, Text> {
  /** The shortest paths id */
  public static final LongConfOption SOURCE_ID =
      new LongConfOption("SimpleShortestPathsVertex.sourceId", 1,
          "The shortest paths id");
  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(SimpleShortestPathComputation.class);

  /**
   * Is this vertex the source id?
   *
   * @param vertex Vertex
   * @return True if the source id
   */
  private boolean isSource(Vertex<LongWritable, ?, ?> vertex) {
    return vertex.getId().get() == SOURCE_ID.get(getConf());
  }

  @Override
  public void compute(
      Vertex<LongWritable, Text, Text> vertex,
      Iterable<Text> messages) throws IOException {
    if (getSuperstep() == 0) {
      //vertex.setValue(new Text("start"));
    }
    double minDist = isSource(vertex) ? 0d : Double.MAX_VALUE;
    for (Text message : messages) {
    	String val[]=message.toString().split("-");
      minDist = Math.min(minDist, new Double(val[1]));
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Vertex " + vertex.getId() + " got minDist = " + minDist +
          " vertex value = " + vertex.getValue());
    }
    
    String vertextval=vertex.getValue().toString();
    double vertxval=0;
    if(vertextval!=null&&vertextval.contains("_")){
    	String []vertexarr=vertextval.split("_");
    	vertextval=vertexarr[0];
    	vertxval=new Double(vertexarr[1]);
    	
    }
    if (minDist < vertxval) {
      vertex.setValue(vertex.getValue());
      for (Edge<LongWritable, Text> edge : vertex.getEdges()) {
    	  String edgeVal=edge.getValue().toString();
    	  double edgedist=0;
    
    	  if(edgeVal!=null&&edgeVal.contains("_")){
    		  String []edgearr=edgeVal.split("_");
    		  edgeVal=edgearr[0];
    		  edgedist=new Double(edgearr[1]);
    	  }
        double distance = minDist + edgedist;
        if (LOG.isDebugEnabled()) {
          LOG.debug("Vertex " + vertex.getId() + " sent to " +
              edge.getTargetVertexId() + " = " + distance);
        }
        sendMessage(vertex.getId(), new Text(edgeVal+"-"+distance));
      }
    }
    vertex.voteToHalt();
  }
}