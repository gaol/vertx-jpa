package io.github.gaol.vertx.ext.jpa;

import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

import javax.persistence.EntityManagerFactory;

import io.reactivex.Maybe;
import io.reactivex.Single;
import io.vertx.codegen.annotations.Fluent;
import io.vertx.codegen.annotations.GenIgnore;
import io.vertx.codegen.annotations.ProxyGen;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Closeable;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.SingleHelper;
import io.vertx.reactivex.servicediscovery.ServiceDiscovery;
import io.vertx.reactivex.servicediscovery.types.EventBusService;
import io.vertx.servicediscovery.Record;
import io.vertx.serviceproxy.ServiceBinder;

/**
 * 
 * @author lgao
 *
 */
@ProxyGen
@VertxGen
public interface JPAService {

    /**
     * The default service name for this JPAService in the {@link ServiceDiscovery}.
     */
    String DEFAULT_SERVICE_NAME = "vertx.ext.jpa.service";

    /**
     * Configure key for the JPA persistent unit name, defaults to {@link DEFAULT_SERVICE_NAME}
     */
    String CONFIG_PERSISTENT_NAME = "jpa.persistent.name";

    /**
     * Configure key for the JPA persistent unit configuration, defaults to an empty {@link JsonObject}.
     *
     * <p>
     * The configuration will override the properties defined in the JPA <i>META-INF/persistence.xml</i>
     */
    String CONFIG_PERSISTENT = "jpa.persistent.config";

    /**
     * JPA Service provider: JPA | Hibernate
     */
    String CONFIG_JPA_PROVIDER = "jpa.service.provider";

    /**
     * Hibernate provider.
     */
    String JPA_PROVIDER_HIBERNATE = "Hibernate";

    /**
     * JPA provider. This is the default
     */
    String JPA_PROVIDER_JPA = "JPA";

    /**
     * Configure key for the service proxy address, defaults to {@link DEFAULT_SERVICE_PROXY_ADDRESS}.
     *
     *<p>
     * This is usually used by the {@link JPAVerticle}.
     */
    String CONFIG_PROXY_ADDRESS = "jpa.service.proxy.address";

    /**
     * The default service proxy address for this JPAService.
     */
    String DEFAULT_SERVICE_PROXY_ADDRESS = "vertx.ext.jpa.service";

    /**
     * The Single to create the JPAService with configuration.
     *
     * @param vertx the Vertx instance
     * @param config The configuration to create the JPAService.
     *    <p> The database configuration is specified here or in the  <i>META-INF/persistence.xml</i>.
     * @return The Single to create the JPAService
     */
    @GenIgnore
    static Single<JPAService> rxCreate(Vertx vertx, JsonObject config) {
        return rxCreate(vertx, DEFAULT_SERVICE_NAME, config);
    }

    /**
     * The Single to create the JPAService with configuration.
     *
     * @param vertx the Vertx instance
     * @param serviceName the service name used to distinguish from other JPAService.
     * @param config The configuration to create the JPAService.
     *    <p> The database configuration is specified here or in the  <i>META-INF/persistence.xml</i>.
     * @return The Single to create the JPAService
     */
    @GenIgnore
    static Single<JPAService> rxCreate(Vertx vertx, String serviceName, JsonObject config) {
        Objects.requireNonNull(vertx, "Vertx is required");
        return SingleHelper.toSingle(handler -> new JPAServiceImpl(vertx, serviceName, config, handler));
    }

    /**
     * The Single to create the JPAService with configuration and a Supplier to produce the EntityManagerFactory.
     *
     * @param vertx the Vertx instance
     * @param serviceName the service name used to distinguish from other JPAService.
     * @param config The configuration to create the JPAService.
     *    <p> The database configuration is specified here or in the  <i>META-INF/persistence.xml</i>.
     * @param emfProvider the Supplier to produce the EntityManagerFactory.
     * @return The Single to create the JPAService
     */
    @GenIgnore
    static Single<JPAService> rxCreate(Vertx vertx, String serviceName, JsonObject config, Supplier<EntityManagerFactory> emfProvider) {
        Objects.requireNonNull(vertx, "Vertx is required");
        return SingleHelper.toSingle(handler -> new JPAServiceImpl(vertx, serviceName, config, emfProvider, handler));
    }

    /**
     * Registers a JPAService onto the specified address.
     *
     * The Registered MessageConsumer will be unregistered when the Vertx Context is closed.
     *
     * @param vertx the Vertx instance
     * @param jpaService the JPAService, it can be retrieved from {@link JPAService.rxCreate} methods.
     * @param serviceAddress the address where the service will be registered on.
     * @return a MessageConsumer which listens to the specified service address.
     */
    @GenIgnore
    static MessageConsumer<JsonObject> rxRegister(Vertx vertx, JPAService jpaService, String serviceAddress) {
        ServiceBinder sb = new ServiceBinder(vertx).setAddress(serviceAddress);
        MessageConsumer<JsonObject> mc = sb.register(JPAService.class, jpaService);
        vertx.getOrCreateContext().addCloseHook(completionHandler -> {
            completionHandler.handle(Future.succeededFuture());
            sb.unregister(mc);
        });
        return mc;
    }

    /**
     * Publishes the JPAService using the specified service name and address.
     *
     *<p>
     *   NOTE: the JPAService should be registered first to the specified address before publishing it.
     *
     * @param discovery the ServiceDiscovery
     * @param serviceName the service name where the service is published to
     * @param serviceAddress the service proxy address underlined the service instance.
     * @return a Single to get a published Record.
     */
    @GenIgnore
    static Single<Record> rxPublish(ServiceDiscovery discovery, String serviceName, String serviceAddress) {
        return discovery.rxPublish(EventBusService.createRecord(serviceName, serviceAddress, JPAService.class.getName()));
    }

    /**
     * Discover the JPAService using the specified service name.
     *
     * @param discovery the ServiceDiscovery
     * @param serviceName the service name where the service is published to
     * @return a Maybe to refer to the JPAService. If wrong service name is specified, no service is returned.
     */
    @GenIgnore
    static Maybe<JPAService> rxDiscovery(ServiceDiscovery discovery, String serviceName) {
        return discovery.rxGetRecord(r -> r.getName().equals(serviceName)).map(r -> discovery.getReference(r).getAs(JPAService.class));
    }

    /**
     * Getting the JPAService Proxy using default proxy address.
     *
     *<p>
     *  NOTE: The JPAService must be registered first. JPAVerticle can be used as the facility to register and publish the service.
     *
     * @param vertx the reactive version of Vertx instance
     * @return the JPAService Proxy
     */
    @GenIgnore
    static io.github.gaol.vertx.ext.jpa.reactivex.JPAService createProxy(Vertx vertx) {
        return createProxy(vertx, DEFAULT_SERVICE_PROXY_ADDRESS);
    }

    /**
     * Getting the JPAService Proxy using specified proxy address.
     *
     *<p>
     *  NOTE: The JPAService must be registered first. JPAVerticle can be used as the facility to register and publish the service.
     *
     * @param vertx the reactive version of Vertx instance
     * @param address the proxy address where the JPAService is registered.
     * @return the JPAService Proxy
     */
    @GenIgnore
    static io.github.gaol.vertx.ext.jpa.reactivex.JPAService createProxy(Vertx vertx, String address) {
        Objects.requireNonNull(vertx, "Vertx is required");
        Objects.requireNonNull(address, "JPAService Address is required");
        return new io.github.gaol.vertx.ext.jpa.reactivex.JPAService(
                new JPAServiceVertxEBProxy(vertx, address));
    }

    /**
     * Query by the defined query name.
     *
     * @param queryName the defined JPA query name
     * @param params the needed parameters for the query
     * @param resultHandler the handler with JsonArray as the result.
     * @return the JPAService itself.
     */
    @Fluent
    JPAService queryName(String queryName, JsonObject params, Handler<AsyncResult<JsonArray>> resultHandler);

    /**
     * Query by the defined query name with additional labels to label the results.
     *
     * @param queryName the defined JPA query name
     * @param params the needed parameters for the query
     * @param labels the labels needed for the keys in the JsonObject for each row
     * @param resultHandler the handler with JsonArray as the result.
     * @return the JPAService itself.
     */
    @Fluent
    JPAService queryNameWithLabels(String queryName, JsonObject params, List<String> labels, Handler<AsyncResult<JsonArray>> resultHandler);

    /**
     * Query by specifying the query string with additional labels to label the results.
     *
     * @param queryString the query string used to query the result.
     * @param params the needed parameters for the query
     * @param resultHandler the handler with JsonArray as the result.
     * @return the JPAService itself.
     */
    @Fluent
    JPAService queryJQL(String queryString, JsonObject params, Handler<AsyncResult<JsonArray>> resultHandler);

    /**
     * Query by specifying the query string.
     *
     * @param queryString the query string used to query the result.
     * @param params the needed parameters for the query
     * @param labels the labels needed for the keys in the JsonObject for each row
     * @param resultHandler the handler with JsonArray as the result.
     * @return the JPAService itself.
     */
    @Fluent
    JPAService queryJQLWithLabels(String queryString, JsonObject params, List<String> labels, Handler<AsyncResult<JsonArray>> resultHandler);

    /**
     * Query by specifying the native SQL query string.
     *
     * @param sqlQueryString the native SQL query string used to query the result.
     * @param params the needed parameters for the query
     * @param labels the labels needed for the keys in the JsonObject for each row
     * @param resultHandler the handler with JsonArray as the result.
     * @return the JPAService itself.
     */
    @Fluent
    JPAService querySQLWithLabels(String sqlQueryString, JsonObject params, List<String> labels, Handler<AsyncResult<JsonArray>> resultHandler);

    /**
     * Query one single result by specifying the defined query name.
     *
     * @param queryName the defined JAP query name.
     * @param params the needed parameters for the query
     * @param resultHandler the handler with JsonObject as the result.
     * @return the JPAService itself.
     */
    @Fluent
    JPAService queryOneName(String queryName, JsonObject params, Handler<AsyncResult<JsonObject>> resultHandler);

    /**
     * Query one single result by specifying the defined query name.
     *
     * @param queryName the defined JAP query name.
     * @param params the needed parameters for the query
     * @param labels the labels needed for the keys in the JsonObject for each row
     * @param resultHandler the handler with JsonObject as the result.
     * @return the JPAService itself.
     */
    @Fluent
    JPAService queryOneNameWithLabels(String queryName, JsonObject params, List<String> labels, Handler<AsyncResult<JsonObject>> resultHandler);

    /**
     * Query one single result by specifying the query string.
     *
     * @param queryString the query string used to query the result.
     * @param params the needed parameters for the query
     * @param resultHandler the handler with JsonObject as the result.
     * @return the JPAService itself.
     */
    @Fluent
    JPAService queryOneJQL(String queryString, JsonObject params, Handler<AsyncResult<JsonObject>> resultHandler);

    /**
     * Query one single result by specifying the query string.
     *
     * @param queryString the query string used to query the result.
     * @param params the needed parameters for the query
     * @param labels the labels needed for the keys in the JsonObject for each row
     * @param resultHandler the handler with JsonObject as the result.
     * @return the JPAService itself.
     */
    @Fluent
    JPAService queryOneJQLWithLabels(String queryString, JsonObject params, List<String> labels, Handler<AsyncResult<JsonObject>> resultHandler);

    /**
     * Query one single result by specifying the entity class name and the primary key.
     *
     * @param entityClassName the entity class name(a class with JPA @entity annotation)
     * @param primaryKey the primary key used to find the entity.
     *          <p> There must be a <b>'value'</b> defined in the primaryKey, like: <b>new JsonObject().put("value", 1000L);</b>
     * @param resultHandler the handler with JsonObject as the result.
     * @return the JPAService itself.
     */
    @Fluent
    JPAService findOne(String entityClassName, JsonObject primaryKey, Handler<AsyncResult<JsonObject>> resultHandler);

    /**
     * Saves an entity by the class name and JSON format of the data. {@link JsonObject#mapFrom(Object)} can be used.
     *
     * @param entityClassName the class name of the entity to be saved
     * @param json the JSON format of the data.
     * @param resultHandler the handler with nothing as the result.
     * @return the JPAService itself.
     */
    @Fluent
    JPAService save(String entityClassName, JsonObject json, Handler<AsyncResult<Void>> resultHandler);

    /**
     * Removes an entity from database by specifying the class name and the primary key.
     *
     * @param entityClassName the class name of the entity to be saved
     * @param primaryKey the primary key used to find the entity.
     *          <p> There must be a <b>'value'</b> defined in the primaryKey, like: <b>new JsonObject().put("value", 1000L);</b>
     * @param resultHandler the handler with nothing as the result.
     * @return the JPAService itself.
     */
    @Fluent
    JPAService remove(String entityClassName, JsonObject primaryKey, Handler<AsyncResult<Void>> resultHandler);

    /**
     * Executes the queries and return the affected rows number.
     *
     * @param queries the queries to be executed.
     *          <p> NOTE: All queries will be executed in one transaction.
     * @param params the parameters needed to passed to each query. Each element in the JsonArray must be a JsonObject.
     * @param resultHandler the handler with affected rows as the result.
     * @return JPAService itself.
     */
    @Fluent
    JPAService executeQueries(List<String> queries, JsonArray params, Handler<AsyncResult<Integer>> resultHandler);

    /**
     * Executes the query and return the affected rows number.
     *
     * @param queryString the query string to be executed.
     * @param params the needed parameters for the query
     * @param resultHandler the handler with affected rows as the result.
     * @return JPAService itself.
     */
    @Fluent
    JPAService executeQuery(String queryString, JsonObject params, Handler<AsyncResult<Integer>> resultHandler);
}