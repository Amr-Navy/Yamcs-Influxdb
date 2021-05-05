package org.yamcs.influxdb;

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
import org.yamcs.influxdb.api.AbstractInfluxdbApi;
import org.yamcs.influxdb.api.ConnectRequest;
import org.yamcs.influxdb.api.ConnectionStatusResposnse;
import org.yamcs.influxdb.api.ParameterHistoryRequest;
import org.yamcs.api.Observer;
import org.yamcs.http.BadRequestException;
import org.yamcs.http.Context;
import org.yamcs.http.InternalServerErrorException;
import org.yamcs.http.NotFoundException;
import org.yamcs.http.api.ManagementApi;
import org.yamcs.http.api.MdbApi;
import org.yamcs.http.api.ParameterReplayListener;


import org.yamcs.logging.Log;
import org.yamcs.parameter.AggregateValue;
import org.yamcs.parameter.ParameterValue;
import org.yamcs.parameter.ParameterValueWithId;
import org.yamcs.parameter.ParameterWithId;
import org.yamcs.parameter.Value;
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

import com.google.protobuf.Timestamp;
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.WriteApi;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;

public class InfluxdbApi extends AbstractInfluxdbApi<Context> {
	InfluxDBClient influxDBClient;
	private static final Log log = new Log(InfluxdbApi.class);

	public InfluxdbApi() {

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

		ConnectionStatusResposnse.Builder b = ConnectionStatusResposnse.newBuilder();

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

		ConnectionStatusResposnse.Builder b = ConnectionStatusResposnse.newBuilder();
		List<Point> Points = new ArrayList<Point>();
		ParameterReplayListener replayListener = new ParameterReplayListener(0, 1000) {
			@Override
			public void onParameterData(ParameterValueWithId pvwid) {
			    Points.add(CreatePoint(pvwid.getParameterValue()));
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
	public Point CreatePoint(ParameterValue pv ) {
	        Timestamp t=  TimeEncoding.toProtobufTimestamp(pv.getGenerationTime()) ;
	        Instant instant = Instant.ofEpochSecond(t.getSeconds(), t.getNanos());
	        Point point = Point.measurement(pv.getParameterQualifiedName()).time(instant,WritePrecision.NS);
	        point.addTag("AquisitionStatus",pv.getAcquisitionStatus().name());
	        point.addTag("Source", pv.getParameter().getDataSource().name());
	        point.addTag("Name", pv.getParameter().getName());
	       // point.addTag("Type", pv.getParameter().getParameterType().getName());
	        point.addTag("Subsystem", pv.getParameter().getSubsystemName());
	        point.addTag("ParameterType", pv.getEngValue().getType().toString());
	        FillPoint(point,pv.getParameter().getName(),pv.getEngValue());

	        
	        return point;
	    }
	public void FillPoint(Point point,String ParameterName,Value val)
	{
	    switch (val.getType()) {
	    case DOUBLE:
	        point.addField(ParameterName+"-value", val.getDoubleValue());
	        //Points.add(point);
	        break;
	    case FLOAT:
	        point.addField(ParameterName+"-value", val.getFloatValue());
	        //Points.add(point);
	        break;
	    case SINT32:
	        point.addField(ParameterName+"-value", val.getSint32Value());
	       // Points.add(point);
	        break;
	    case SINT64:
	        point.addField(ParameterName +"-value", val.getSint64Value());
	       // Points.add(point);
	        break;
	    case UINT32:
	        point.addField(ParameterName +"-value", val.getUint32Value() & 0xFFFFFFFFL);
	       // Points.add(point);
	        break;
	    case UINT64:
	        point.addField(ParameterName+"-value", val.getUint64Value());
	       // Points.add(point);
	        break;
	    case STRING:
	        point.addField(ParameterName +"-value", val.getStringValue());
	      //  Points.add(point);
	        break;
	    case TIMESTAMP:
	        point.addField(ParameterName+ "-value", val.getTimestampValue());
	        //Points.add(point);
	        break;
	    case BOOLEAN:
	        point.addField(ParameterName+ "-value", val.getBooleanValue());
	       // Points.add(point);
	        break;
	    case AGGREGATE:
	      
	        int size = ((AggregateValue) (val)).getMemberNames().size();
	        for (int i = 0; i < size; i++) 
	        {
	            String name =((AggregateValue) (val)).getMemberName(i);
	            Value aggval=  ((AggregateValue) (val)).getMemberValue(i);
	            FillPoint(point,name,aggval);
	        }
	        break;     
	    default:
	        // log.warn("Unexpected value type {}", pv.getEngValue().getType());

	    }
	}
	public void WritetoDB(List<Point> items) {

		try (WriteApi writeApi = influxDBClient.getWriteApi()) {

			writeApi.writePoints(items);

		}

		finally {

		}

	}

	private void retrieveParameterData(ParameterArchive parchive, ParameterWithId pid,
			MultipleParameterValueRequest mpvr, ParameterReplayListener replayListener)
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
