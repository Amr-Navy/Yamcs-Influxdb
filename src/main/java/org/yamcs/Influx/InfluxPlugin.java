package org.yamcs.Influx;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.yamcs.Plugin;
import org.yamcs.PluginException;
import org.yamcs.YConfiguration;
import org.yamcs.YamcsServer;
import org.yamcs.http.Handler;
import org.yamcs.http.HttpRequestHandler;
import org.yamcs.http.HttpServer;
import org.yamcs.logging.Log;
import org.yamcs.security.SystemPrivilege;

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.QueryStringDecoder;


public class InfluxPlugin implements Plugin {

    public static final SystemPrivilege PRIV_GET_METRICS = new SystemPrivilege("Influx.GetParameters");

    private static final Log log = new Log(InfluxPlugin.class);

    @Override
    public void onLoad(YConfiguration config) throws PluginException {
        YamcsServer yamcs = YamcsServer.getServer();
        yamcs.getSecurityStore().addSystemPrivilege(PRIV_GET_METRICS);
        List<HttpServer> httpServers = yamcs.getGlobalServices(HttpServer.class);
        if (httpServers.isEmpty()) {
            log.warn("Can't mount metrics endpoint. Yamcs does not appear to be running an HTTP Server.");
            return;
        }

        HttpServer httpServer = httpServers.get(0);
    

        try (InputStream in = getClass().getResourceAsStream("/yamcs-Influx.protobin")) {
            httpServer.getProtobufRegistry().importDefinitions(in);
        } catch (IOException e) {
            throw new PluginException(e);
        }

        httpServer.addApi(new InfluxApi());

    }

}
