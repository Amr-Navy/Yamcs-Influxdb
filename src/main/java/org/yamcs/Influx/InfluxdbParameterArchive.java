package org.yamcs.Influx;

import java.time.Instant;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;

import org.rocksdb.RocksDBException;
import org.yamcs.AbstractYamcsService;
import org.yamcs.ConfigurationException;
import org.yamcs.InitException;
import org.yamcs.Processor;
import org.yamcs.Spec;
import org.yamcs.Spec.OptionType;
import org.yamcs.YConfiguration;
import org.yamcs.YamcsServer;
import org.yamcs.parameter.ParameterConsumer;
import org.yamcs.parameter.ParameterValue;
import org.yamcs.parameter.Value;
import org.yamcs.utils.TimeEncoding;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.WriteApi;
import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import com.influxdb.client.write.events.BackpressureEvent;
import com.influxdb.client.write.events.WriteErrorEvent;
import com.influxdb.client.write.events.WriteSuccessEvent;


public class InfluxdbParameterArchive extends AbstractYamcsService implements ParameterConsumer {
	int subscriptionId;
	Processor realtimeProcessor;
	String processorName = "realtime";
    InfluxDBClient influxDBClient;
    String token = "";
    String org = "";
    String bucket = "";
    String Link = "";
    
    @Override
    public Spec getSpec() {
        Spec spec = new Spec();
//
        spec.addOption("processorName", OptionType.STRING).withDefault("realtime");
        spec.addOption("org", OptionType.STRING);
        spec.addOption("token", OptionType.STRING);
        spec.addOption("bucket", OptionType.STRING);
        spec.addOption("Link", OptionType.STRING).withDefault("http://localhost:8086\\");

        return spec;
    }

    @Override
    public void init(String yamcsInstance, String serviceName, YConfiguration config) throws InitException {
        super.init(yamcsInstance, serviceName, config);
       
        if(config.containsKey("processorName"))
        {
        	 processorName = config.getString("processorName", processorName);
        }
        else
        {
        	processorName="realtime";
        }
        if(config.containsKey("token"))
        {
        	token = config.getString("token");
        }
        if(config.containsKey("bucket"))
        {
            bucket = config.getString("bucket");
        }
        if(config.containsKey("org"))
        {
        	org = config.getString("org");
        }

        if(config.containsKey("Link"))
        {
        	Link=config.getString("Link");
        }
        
        try {
        	   influxDBClient = InfluxDBClientFactory.create(Link, token.toCharArray(), org, bucket);
        } catch (Exception e) {
            throw new InitException("Failed to open Influxdb", e);
        }
     


    }

    @Override
    protected void doStart() {
    	realtimeProcessor = YamcsServer.getServer().getProcessor(yamcsInstance, processorName);
        if (realtimeProcessor == null) {
            throw new ConfigurationException("No processor named '" + processorName);
        }
       subscriptionId = realtimeProcessor.getParameterRequestManager().subscribeAll(this);
       notifyStarted();
    }

    @Override
    protected void doStop() {
        log.debug("Stopping ParameterArchive service for instance {}", yamcsInstance);
        try {
        	 realtimeProcessor.getParameterRequestManager().unsubscribeAll(subscriptionId);
        	 influxDBClient.close();
        } catch (Exception e) {
            log.error("Error stopping realtime filler", e);
            notifyFailed(e);
            return;
        }
        notifyStopped();
    }



    @Override
 public String toString() 
    {
   return "";
    }

public  void WritetoDB(List<Point> items)
{

	 try(WriteApi writeApi = influxDBClient.getWriteApi()) {
		 
    writeApi.writePoints(items);
    
	 }
	 
	 finally {
		
	}


}
	@Override
	public void updateItems(int subscriptionId, List<ParameterValue> items) {
		List<Point> Points = new ArrayList<Point>();

	if(!items.isEmpty())
	{
		for (ParameterValue pv : items) 
		{
	       

	            if (pv.getParameterQualifiedName() == null) {
	                log.warn("No qualified name for parameter value {}, ignoring", pv);
	                continue;
	            }
	            Value engValue = pv.getEngValue();
	            if (engValue == null) {
	                log.warn("Ignoring parameter without engineering value: {} ", pv.getParameterQualifiedName());
	            }
	            else
	            {
	            	 Instant t= Instant.ofEpochMilli(pv.getGenerationTime()).minusMillis(37000);
	            		  Point point = Point.measurement(pv.getParameterQualifiedName()).time(t.toEpochMilli(), WritePrecision.MS);
	            		  
	            		  switch (pv.getEngValue().getType()) 
	            		  {
	 	                 case DOUBLE:
	 	                	point.addField("value", pv.getEngValue().getDoubleValue());
	 	                	 Points.add(point);
	 	                     break;
	 	                 case FLOAT:
		 		 	        point.addField("value", pv.getEngValue().getFloatValue());
		 		 	        Points.add(point);
	 	                     break;
	 	                 case SINT32:
	 	                	point.addField("value", pv.getEngValue().getSint32Value());
	 	                	 Points.add(point);
	 	                     break;
	 	                 case SINT64:
	 	                	point.addField("value",  pv.getEngValue().getSint64Value());
	 	                	 Points.add(point);
	 	                     break;
	 	                 case UINT32:
	 	                	point.addField("value", pv.getEngValue().getUint32Value() & 0xFFFFFFFFL);
	 	                	 Points.add(point);
	 	                     break;
	 	                 case UINT64:
	 	                	point.addField("value", pv.getEngValue().getUint64Value());
	 	                	 Points.add(point);
	 	                     break;
	 	                 case STRING:
	 	                	point.addField("value", pv.getEngValue().getStringValue());
	 	                	 Points.add(point);
	 	                	break;
	 	                 case TIMESTAMP:
	 	                	point.addField("value", pv.getEngValue().getTimestampValue());
	 	                	 Points.add(point);
	 	                	break;
	 	                 case BOOLEAN:
	 	                	point.addField("value", pv.getEngValue().getBooleanValue());
	 	                	Points.add(point);
	 	                	break;
	 	                 default:
	 	                    // log.warn("Unexpected value type {}", pv.getEngValue().getType());	
	 	                   
	  	                 }

	           
	        }
		}
		
		WritetoDB(Points);
	}
	
	}


}


