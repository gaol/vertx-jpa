package com.spidercoding.vertx.jpa;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.reactivex.Single;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.CompletableHelper;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.servicediscovery.ServiceDiscovery;
import io.vertx.servicediscovery.Record;
import io.vertx.servicediscovery.ServiceDiscoveryOptions;

public class JPAVerticle extends AbstractVerticle {

    public static final String CONFIG_JPA_SERVICE_ADDRESS ="jpa.service.address";
    public static final String CONFIG_PUBLISH_JPA_SERVICE = "jpa.service.publish";
    public static final String CONFIG_JPA_SERVICE_NAME = "jpa.service.name";
    public static final String CONFIG_DISCOVERY_OPTIONS = "vertx.service.discovery.options";

    private static final Logger logger = LoggerFactory.getLogger(JPAVerticle.class);

    private ServiceDiscovery discovery;
    private Record record;

    @Override
    public void start(Future<Void> startFuture) throws Exception {
        JsonObject config = config();
        String jpaAddress = config.getString(CONFIG_JPA_SERVICE_ADDRESS, JPAService.DEFAULT_SERVICE_PROXY_ADDRESS);
        boolean publishService = config.getBoolean(CONFIG_PUBLISH_JPA_SERVICE, true);
        String jpaServiceName = config.getString(CONFIG_JPA_SERVICE_NAME, JPAService.DEFAULT_SERVICE_NAME);

        JPAService.rxCreate(vertx.getDelegate(), jpaServiceName, config)
            .<Record>flatMap(jpaService -> {
                JPAService.rxRegister(getVertx(), jpaService, jpaAddress);
                logger.info("Registered JPA service on address: {}", jpaAddress);
    
                if (publishService) {
                    ServiceDiscoveryOptions discoveryOptions = new ServiceDiscoveryOptions(config.getJsonObject(CONFIG_DISCOVERY_OPTIONS, new JsonObject()));
                    this.discovery = ServiceDiscovery.create(vertx, discoveryOptions);
                    return JPAService.rxPublish(discovery, jpaServiceName, jpaAddress);
                }
                return Single.just(new Record());
            }).subscribe((r, e) -> {
                if (e != null) {
                    startFuture.fail(e);
                } else {
                    this.record = r;
                    if (jpaServiceName.equals(r.getName())) {
                        logger.info("JPAService: {} is created at proxy address: {}, and published at: {}", jpaServiceName, jpaAddress, r.getLocation().toString());
                    } else {
                        logger.info("JPAService: {} is created at proxy address: {}, but not published.", jpaServiceName, jpaAddress);
                    }
                    startFuture.complete();
                }
            });
    }

    @Override
    public void stop(Future<Void> stopFuture) throws Exception {
        if (this.discovery != null && this.record != null) {
            this.discovery
                .rxUnpublish(this.record.getRegistration())
                .subscribe(CompletableHelper.toObserver(stopFuture));
        } else {
            stopFuture.complete();
        }
    }

}