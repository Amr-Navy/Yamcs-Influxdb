package org.yamcs.Influx;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.rocksdb.RocksDBException;
import org.yamcs.YamcsServerInstance;
import org.yamcs.Influx.api.AbstractInfluxApi;
import org.yamcs.Influx.api.ConnectRequest;
import org.yamcs.Influx.api.ConnectionStatusResposnse;
import org.yamcs.Influx.api.ParameterHistoryRequest;
import org.yamcs.api.Observer;
import org.yamcs.http.BadRequestException;
import org.yamcs.http.Context;
import org.yamcs.http.InternalServerErrorException;
import org.yamcs.http.NotFoundException;
import org.yamcs.http.api.ManagementApi;
import org.yamcs.http.api.MdbApi;
import org.yamcs.http.api.ParameterReplayListener;

import org.yamcs.http.api.StreamArchiveApi;

import org.yamcs.logging.Log;

import org.yamcs.parameter.ParameterValue;
import org.yamcs.parameter.ParameterValueWithId;
import org.yamcs.parameter.ParameterWithId;
import org.yamcs.parameterarchive.ConsumerAbortException;
import org.yamcs.parameterarchive.MultiParameterDataRetrieval;
import org.yamcs.parameterarchive.MultipleParameterValueRequest;
import org.yamcs.parameterarchive.ParameterArchive;
import org.yamcs.parameterarchive.ParameterGroupIdDb;
import org.yamcs.parameterarchive.ParameterId;
import org.yamcs.parameterarchive.ParameterIdDb;
import org.yamcs.parameterarchive.ParameterIdValueList;
import org.yamcs.protobuf.Yamcs.NamedObjectId;
import org.yamcs.utils.DecodingException;
import org.yamcs.utils.ExceptionUtil;
import org.yamcs.utils.IntArray;
import org.yamcs.utils.MutableLong;
import org.yamcs.utils.TimeEncoding;
import org.yamcs.xtce.XtceDb;
import org.yamcs.xtceproc.XtceDbFactory;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.WriteApi;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;



public class InfluxApi extends AbstractInfluxApi<Context> {
	InfluxDBClient influxDBClient;
	private static final Log log = new Log(InfluxApi.class);
    public InfluxApi() {
    	
    }

	@Override
	public void connectToDB(Context ctx, ConnectRequest request, Observer<ConnectionStatusResposnse> observer) {
		
		

        if (!request.hasBucket()) {
            throw new BadRequestException("No Bucket name was specified");
        }
        String Bucket = request.getBucket();
        
        if (!request.hasOrg()) {
            throw new BadRequestException("Invalid Organization name");
        }
        String org = request.getOrg();
        if (!request.hasToken()) {
            throw new BadRequestException("No Token was specified");
        }
        String Token = request.getToken();
    
        if (!request.hasLink()) {
            throw new BadRequestException("No Token was specified");
        }
        String Link = request.getLink();
        
   
        ConnectionStatusResposnse.Builder b =  ConnectionStatusResposnse.newBuilder();
        
        CompletableFuture<String> cf = CompletableFuture.supplyAsync(() -> {
            try {

            	influxDBClient = InfluxDBClientFactory.create(Link, Token.toCharArray(), org, Bucket);
                return influxDBClient.health().getMessage();
               
            } catch (Exception e) {
                throw new BadRequestException(e.getMessage());
            }
        });

        cf.whenComplete((v, error) -> {
            if (error == null) {
                observer.complete(b.setName(influxDBClient.health().getMessage()).build());
            } else {
                Throwable t = ExceptionUtil.unwind(error);
                observer.completeExceptionally(new InternalServerErrorException(t));
            }
        });
	}

	@Override
	public void archiveParameterHistory(Context ctx, ParameterHistoryRequest request,
			Observer<ConnectionStatusResposnse> observer) {

	        YamcsServerInstance ysi = ManagementApi.verifyInstanceObj(request.getInstance());

	        XtceDb mdb = XtceDbFactory.getInstance(ysi.getName());
	        ParameterWithId requestedParamWithId = MdbApi.verifyParameterWithId(ctx, mdb, request.getName());

	        NamedObjectId requestedId = requestedParamWithId.getId();

	

	        long start = 0;
	        if (request.hasStart()) {
	            start = TimeEncoding.fromProtobufTimestamp(request.getStart());
	        }
	        long stop = TimeEncoding.getWallclockTime();
	        if (request.hasStop()) {
	            stop = TimeEncoding.fromProtobufTimestamp(request.getStop());
	        }


	        ParameterArchive parchive = getParameterArchive(ysi);
	        ParameterIdDb piddb = parchive.getParameterIdDb();
	        IntArray pidArray = new IntArray();
	        IntArray pgidArray = new IntArray();
	        String qn = requestedParamWithId.getQualifiedName();
	        ParameterId[] pids = piddb.get(qn);

	        BitSet retrieveRawValues = new BitSet();
	        if (pids != null) {
	            ParameterGroupIdDb pgidDb = parchive.getParameterGroupIdDb();
	            for (ParameterId pid : pids) {
	                int[] pgids = pgidDb.getAllGroups(pid.pid);
	                for (int pgid : pgids) {
	                    if (pid.getRawType() != null) {
	                        retrieveRawValues.set(pidArray.size());
	                    }
	                    pidArray.add(pid.pid);
	                    pgidArray.add(pgid);
	                }
	            }

	            if (pidArray.isEmpty()) {
	                log.error("No parameter group id found in the parameter archive for {}", qn);
	                throw new NotFoundException();
	            }
	        } else {
	            log.warn("No parameter id found in the parameter archive for {}", qn);
	        }
	        String[] pnames = new String[pidArray.size()];
	        Arrays.fill(pnames, requestedParamWithId.getQualifiedName());
	        MultipleParameterValueRequest mpvr = new MultipleParameterValueRequest(start, stop, pnames, pidArray.toArray(),
	                pgidArray.toArray(), retrieveRawValues, true);

	        ConnectionStatusResposnse.Builder b =  ConnectionStatusResposnse.newBuilder();
	        List<Point> Points = new ArrayList<Point>();
	        ParameterReplayListener replayListener = new ParameterReplayListener(0, 1000) {
	            @Override
	            public void onParameterData(ParameterValueWithId pvwid) {
	            	  
	                 //  pvwid.toGbpParameterValue();
	                   Instant t= Instant.ofEpochMilli(pvwid.getParameterValue().getGenerationTime()).minusMillis(37000);
	            		  Point point = Point.measurement(pvwid.getParameterValue().getParameterQualifiedName()).time(t.toEpochMilli(), WritePrecision.MS);
	            		  
	            		  switch (pvwid.toGbpParameterValue().getEngValue().getType()) 
	            		  {
	 	                 case DOUBLE:
	 	                	point.addField("value", pvwid.getParameterValue().getEngValue().getDoubleValue());
	 	                	 Points.add(point);
	 	                     break;
	 	                 case FLOAT:
		 		 	        point.addField("value", pvwid.getParameterValue().getEngValue().getFloatValue());
		 		 	        Points.add(point);
	 	                     break;
	 	                 case SINT32:
	 	                	point.addField("value", pvwid.getParameterValue().getEngValue().getSint32Value());
	 	                	 Points.add(point);
	 	                     break;
	 	                 case SINT64:
	 	                	point.addField("value",  pvwid.getParameterValue().getEngValue().getSint64Value());
	 	                	 Points.add(point);
	 	                     break;
	 	                 case UINT32:
	 	                	point.addField("value", pvwid.getParameterValue().getEngValue().getUint32Value() & 0xFFFFFFFFL);
	 	                	 Points.add(point);
	 	                     break;
	 	                 case UINT64:
	 	                	point.addField("value", pvwid.getParameterValue().getEngValue().getUint64Value());
	 	                	 Points.add(point);
	 	                     break;
	 	                 case STRING:
	 	                	point.addField("value", pvwid.getParameterValue().getEngValue().getStringValue());
	 	                	 Points.add(point);
	 	                	break;
	 	                 case TIMESTAMP:
	 	                	point.addField("value", pvwid.getParameterValue().getEngValue().getTimestampValue());
	 	                	 Points.add(point);
	 	                	break;
	 	                 case BOOLEAN:
	 	                	point.addField("value", pvwid.getParameterValue().getEngValue().getBooleanValue());
	 	                	Points.add(point);
	 	                	break;
	 	                 default:
	 	                    // log.warn("Unexpected value type {}", pv.getEngValue().getType());	
	 	                   
	  	                 }
	            		 
	           
	            		  
	        }
	        
	            @Override
	            public void replayFinished() {
	                throw new UnsupportedOperationException();
	            }

	            @Override
	            public void replayFailed(Throwable t) {
	                throw new UnsupportedOperationException();
	            }
	        };

	        CompletableFuture<Void> cf = CompletableFuture.runAsync(() -> {
	        try {
	           
	            retrieveParameterData(parchive, requestedParamWithId, mpvr, replayListener);
	            
	        } catch (DecodingException | RocksDBException | IOException e) {
	            throw new InternalServerErrorException(e);
	        }
	       
	       
	        });
	        
	        cf.whenComplete((v, error) -> {
	            if (error == null) {
	            	WritetoDB(Points);
	            	observer.complete(b.setName(influxDBClient.health().getMessage()).build());
	            } else {
	                Throwable t = ExceptionUtil.unwind(error);
	                log.error("Error when creating Connection");
	                observer.completeExceptionally(new InternalServerErrorException(t));
	            }
	        });
	        
	    }
	public  void WritetoDB(List<Point> items)
	{

		 try(WriteApi writeApi = influxDBClient.getWriteApi()) {
			 
	    writeApi.writePoints(items);
	    
		 }
		 
		 finally {
			
		}


	}
	private void retrieveParameterData(ParameterArchive parchive, ParameterWithId pid,
            MultipleParameterValueRequest mpvr,ParameterReplayListener replayListener)
            throws RocksDBException, DecodingException, IOException {

        MutableLong lastParameterTime = new MutableLong(TimeEncoding.INVALID_INSTANT);
        Consumer<ParameterIdValueList> consumer = new Consumer<ParameterIdValueList>() {
            boolean first = true;

            @Override
            public void accept(ParameterIdValueList pidvList) {
                lastParameterTime.setLong(pidvList.getValues().get(0).getGenerationTime());

                ParameterValue pv = pidvList.getValues().get(0);
                replayListener.update(new ParameterValueWithId(pv, pid.getId()));
                if (replayListener.isReplayAbortRequested()) {
                    throw new ConsumerAbortException();
                }
            }
        };
        MultiParameterDataRetrieval mpdr = new MultiParameterDataRetrieval(parchive, mpvr);
        mpdr.retrieve(consumer);


    }
	    private ParameterArchive getParameterArchive(YamcsServerInstance ysi) throws BadRequestException {
	        List<ParameterArchive> l = ysi.getServices(ParameterArchive.class);

	        if (l.isEmpty()) {
	            throw new BadRequestException("ParameterArchive not configured for this instance");
	        }

	        return l.get(0);
		
	}





}
