package com.t.app;

import io.rsocket.ConnectionSetupPayload;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import io.rsocket.core.RSocketServer;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.DefaultPayload;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

import java.util.function.Consumer;
import java.util.function.Function;

public class Server {

    public static void main(String[] args) throws Exception {




        RSocketServer.create(new SocketAcceptor() {
            @Override
            public Mono<RSocket> accept(ConnectionSetupPayload setup, RSocket sendingSocket) {


                System.out.println("getDataUtf8 : "+setup.getDataUtf8());
                System.out.println("getData : "+setup.getData());
                System.out.println("getMetadataUtf8 : "+setup.getMetadataUtf8());
                System.out.println("getMetadata : "+setup.getMetadata());


                return Mono.just(new RSocket() {
                    @Override
                    public Mono<Void> fireAndForget(Payload payload) {
                        return RSocket.super.fireAndForget(payload);
                    }

                    @Override
                    public Mono<Payload> requestResponse(Payload payload) {
                        return RSocket.super.requestResponse(payload);
                    }

                    @Override
                    public Flux<Payload> requestStream(Payload payload) {
                        return RSocket.super.requestStream(payload);
                    }

                    @Override
                    public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
                        Consumer<FluxSink<Payload>> pushMessage = new Consumer<>() {
                            @Override
                            public void accept(FluxSink<Payload> payloadFluxSink) {
                                new Thread(new Runnable() {
                                    @Override
                                    public void run() {
                                        while (true) {
                                            try {
                                                Thread.sleep(1000);
                                                payloadFluxSink.next(DefaultPayload.create("push message"));
                                            } catch (InterruptedException e) {
                                            }
                                        }
                                    }
                                }).start();
                            }
                        };
                        Flux<Payload> r = Flux.create(pushMessage);

                        System.out.println("requestChannel");
                        payloads.subscribe(new Subscriber<Payload>() {
                            @Override
                            public void onSubscribe(Subscription s) {
                                s.request(Long.MAX_VALUE);
                            }

                            @Override
                            public void onNext(Payload payload) {
                                System.out.println("onNext " + payload.getDataUtf8());


                            }

                            @Override
                            public void onError(Throwable t) {

                            }

                            @Override
                            public void onComplete() {

                            }
                        });
                        return r;
                    }

                    @Override
                    public Mono<Void> metadataPush(Payload payload) {
                        return RSocket.super.metadataPush(payload);
                    }

                    @Override
                    public double availability() {
                        return RSocket.super.availability();
                    }

                    @Override
                    public void dispose() {
                        RSocket.super.dispose();
                    }

                    @Override
                    public boolean isDisposed() {
                        return RSocket.super.isDisposed();
                    }

                    @Override
                    public Mono<Void> onClose() {
                        return RSocket.super.onClose();
                    }
                });
            }
        }).bind(TcpServerTransport.create("192.168.20.91", 23102)).block();

        Thread.currentThread().join();
    }
}
