package org.yamcs.influxdb;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

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
import org.yamcs.protobuf.Yamcs.Value.Type;
import org.yamcs.utils.TimeEncoding;

import com.google.protobuf.Timestamp;
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.WriteApi;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;

import org.yamcs.parameter.AggregateValue;

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

        if (config.containsKey("processorName")) {
            processorName = config.getString("processorName", processorName);
        } else {
            processorName = "realtime";
        }
        if (config.containsKey("token")) {
            token = config.getString("token");
        }
        if (config.containsKey("bucket")) {
            bucket = config.getString("bucket");
        }
        if (config.containsKey("org")) {
            org = config.getString("org");
        }

        if (config.containsKey("Link")) {
            Link = config.getString("Link");
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
    public String toString() {
        return "";
    }

    public void WritetoDB(List<Point> items) {

        try (WriteApi writeApi = influxDBClient.getWriteApi()) {

            writeApi.writePoints(items);

        }

        finally {

        }

    }

    @Override
    public void updateItems(int subscriptionId, List<ParameterValue> items) {
        List<Point> Points = new ArrayList<Point>();

        if (!items.isEmpty()) {
            for (ParameterValue pv : items) {

                if (pv.getParameterQualifiedName() == null) {
                    log.warn("No qualified name for parameter value {}, ignoring", pv);
                    continue;
                }
                Value engValue = pv.getEngValue();
                if (engValue == null) {
                    log.warn("Ignoring parameter without engineering value: {} ", pv.getParameterQualifiedName());
                } else {
                  
                    Points.add(CreatePoint(pv));

                }
            }

            WritetoDB(Points);
        }

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
}
