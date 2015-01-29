package com.rii.giraph.learn;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.io.FileUtils;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.io.formats.GiraphFileInputFormat;
import org.apache.giraph.io.formats.IdWithValueTextOutputFormat;
import org.apache.giraph.io.formats.JsonBase64VertexInputFormat;
import org.apache.giraph.io.formats.JsonLongDoubleFloatDoubleVertexInputFormat;
import org.apache.giraph.job.GiraphJob;
import org.apache.giraph.utils.ConfigurationUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.rii.giraph.learn.input.GiraphJsonInput;


public class GiraphOnHadoopRunner implements Tool {

	private Configuration conf;
	  static {
		    Configuration.addDefaultResource("giraph-site.xml");
		  }

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	@Override
	public int run(String[] arg0) throws Exception {
		if (getConf() == null)
			conf = new Configuration();

		GiraphConfiguration giraphConf = new GiraphConfiguration(getConf());
		giraphConf.setComputationClass(SimpleShortestPathComputation.class);
		//giraphConf.setVertexInputFormatClass(JsonLongDoubleFloatDoubleVertexInputFormat.class);
		giraphConf.setVertexInputFormatClass(GiraphJsonInput.class);
		giraphConf.setVertexOutputFormatClass(IdWithValueTextOutputFormat.class);
		
		//GiraphFileInputFormat.addVertexInputPath(giraphConf, new Path("/home/user/workspace/GiraphProject/input/graph-input"));
		GiraphFileInputFormat.addVertexInputPath(giraphConf, new Path("/home/user/workspace/GiraphProject/input/graph-json-input"));
		
		
		giraphConf.setWorkerConfiguration(0, 1, 100.0f);
		giraphConf.SPLIT_MASTER_WORKER.set(giraphConf, false);
		giraphConf.setLocalTestMode(true);
		
		GiraphJob job =new GiraphJob(giraphConf,getClass().getName());
		File directory=new File("/home/user/workspace/GiraphProject/output");		
		
		FileUtils.deleteDirectory(directory);
		FileOutputFormat.setOutputPath(job.getInternalJob(), new Path("/home/user/workspace/GiraphProject/output"));
		return job.run(true) ? 0 : 1;
		
		//CommandLine cmd = ConfigurationUtils.parseArgs(giraphConf, arg0);
		//if (null == cmd)
			//return 0;

		//final String vertexClassName = arg0[0];
		//final String jobName = "Giraph: " + vertexClassName;
		//GiraphJob job = new GiraphJob(giraphConf, jobName);
		//prepareHadoopMRJob(job, cmd);
		//return 0;
	}

	private void prepareHadoopMRJob(GiraphJob job, CommandLine cmd) throws URISyntaxException {
		if (cmd.hasOption("vof") || cmd.hasOption("eof")) {
			if (cmd.hasOption("op")) {
				FileOutputFormat.setOutputPath(job.getInternalJob(), new Path(
						cmd.getOptionValue("op")));
			}

		}
		
		if(cmd.hasOption("cf")){
			DistributedCache.addCacheFile(new URI(cmd.getOptionValue("cf")), job.getConfiguration());
		}
	}
	
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new GiraphOnHadoopRunner(), args));
	}

}
