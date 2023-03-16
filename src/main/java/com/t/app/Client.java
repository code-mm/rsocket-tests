package com.t.app;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.core.RSocketConnector;
import io.rsocket.core.Resume;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.metadata.WellKnownMimeType;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.client.WebsocketClientTransport;
import io.rsocket.util.ByteBufPayload;
import io.rsocket.util.DefaultPayload;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.publisher.*;
import reactor.util.retry.Retry;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public class Client {


    public static ByteBuf routeByteBuf(String routePath) {
        byte[] routePathBytes = routePath.getBytes();
        ByteBuf buf = Unpooled.buffer();
        buf.writeByte(routePathBytes.length);
        buf.writeBytes(routePathBytes);
        return buf;
    }
    public static void main(String[] args) {

        RSocket socket =
                RSocketConnector
                        .create()
                        .resume(new Resume().token(new Supplier<ByteBuf>() {
                            @Override
                            public ByteBuf get() {
                                return ByteBufPayload.create("tetwset").data();
                            }
                        }))
                        .reconnect(Retry.max(
                                1000
                        ))
                        .payloadDecoder(PayloadDecoder.ZERO_COPY)
                        .setupPayload(Mono.create(new Consumer<MonoSink<Payload>>() {
                            @Override
                            public void accept(MonoSink<Payload> payloadMonoSink) {

                                payloadMonoSink.success(DefaultPayload.create("ccc","bbb"));
                                payloadMonoSink.success(DefaultPayload.create("ccc","bbb"));
                                payloadMonoSink.success(DefaultPayload.create("ccc","bbb"));
                                payloadMonoSink.success(DefaultPayload.create("ccc","bbb"));
                                payloadMonoSink.success();
                            }
                        }))
//                        .setupPayload(DefaultPayload.create("user_id","aaaa"))
                        .dataMimeType(WellKnownMimeType.TEXT_PLAIN.getString())
                        .metadataMimeType(WellKnownMimeType.TEXT_PLAIN.getString())
                        .connectWith(TcpClientTransport.create("192.168.20.91", 23102))
                        .block();

        socket.metadataPush(DefaultPayload.create("test_user_id"));

        socket.requestChannel(Flux.create(new Consumer<FluxSink<Payload>>() {
            @Override
            public void accept(FluxSink<Payload> payloadFluxSink) {
                new Thread(new Runnable() {
                    @Override
                    public void run() {
                        while (true) {
                            try {
                                Thread.sleep(1000);
                                payloadFluxSink.next(DefaultPayload.create("aa"));
                            } catch (InterruptedException e) {
                            }
                        }
                    }
                }).start();
                payloadFluxSink.next(DefaultPayload.create("aa"));
            }
        }).map(new Function<Payload, Payload>() {
            @Override
            public Payload apply(Payload payload) {
                return payload;
            }
        })).doOnNext(new Consumer<Payload>() {
            @Override
            public void accept(Payload payload) {
                System.out.println("doOnNext" + payload.getDataUtf8());
            }
        }).doFinally(new Consumer<SignalType>() {
            @Override
            public void accept(SignalType signalType) {
                socket.dispose();
            }
        }).then().block();
    }
}
