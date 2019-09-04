package io.github.gaol.vertx.ext.jpa;


import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import javax.persistence.Persistence;
import javax.persistence.Query;

import io.vertx.core.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.reactivex.Completable;
import io.reactivex.CompletableSource;
import io.reactivex.Flowable;
import io.reactivex.Single;
import io.reactivex.SingleSource;
import io.reactivex.functions.Function;
import io.reactivex.schedulers.Schedulers;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.shareddata.LocalMap;
import io.vertx.core.shareddata.Shareable;
import io.vertx.reactivex.CompletableHelper;
import io.vertx.reactivex.RxHelper;
import io.vertx.reactivex.SingleHelper;

class JPAServiceImpl implements JPAService {

    private static final Logger logger = LoggerFactory.getLogger(JPAServiceImpl.class);

    private static final String LOCAL_MAP = "__vertx.ext.entitymanagerfactory.map";

    private final Vertx vertx;

    private EntityManagerFactoryHolder emfHolder;
    private ExecutorService exec;

    // maybe multiple EntityManagerFactory in one application, like for different databases.
    private final LocalMap<String, EntityManagerFactoryHolder> map;

    // serviceName should be global unique, each for one SessionFactory.
    private final String serviceName;

    JPAServiceImpl(Vertx vertx, String serviceName, JsonObject config, Handler<AsyncResult<JPAService>> readyHandler) {
        this(vertx, serviceName, config, new EntityManagerFactorySupplier(config), readyHandler);
    }

    JPAServiceImpl(Vertx vertx, String serviceName, JsonObject config, Supplier<EntityManagerFactory> emfSupplier, Handler<AsyncResult<JPAService>> readyHandler) {
        super();
        this.vertx = vertx;
        // configuration used to produce the EntityManagerFactory
        // including extra configurations as well needed for this Service.
        JsonObject theConfig = config == null ? new JsonObject() : config;
        this.serviceName = serviceName == null ? theConfig.getString(CONFIG_PERSISTENT_NAME, DEFAULT_SERVICE_NAME) : serviceName;
        this.map = vertx.sharedData().getLocalMap(LOCAL_MAP);
        Promise<JPAService> promise = Promise.promise();
        promise.future().setHandler(readyHandler);
        synchronized (this.vertx) {
            this.emfHolder = map.get(this.serviceName);
            if (this.emfHolder == null) {
                this.emfHolder = new EntityManagerFactoryHolder();
                map.put(this.serviceName, this.emfHolder);
                vertx.executeBlocking(f -> {
                    try {
                        emfHolder.emf = emfSupplier.get();
                        setupCloseHook();
                        f.complete(this);
                    } catch (Exception e) {
                        f.fail(e);
                    }
                }, promise);
            } else {
                emfHolder.incRefCount();
                promise.complete(this);
            }
            this.exec = this.emfHolder.exec();
        }
    }

    private void setupCloseHook() {
        Context ctx = io.vertx.core.Vertx.currentContext();
        if (ctx != null && ctx.owner() == vertx) {
            ctx.addCloseHook(emfHolder::close);
        }
    }

    private static class EntityManagerFactorySupplier implements Supplier<EntityManagerFactory> {
        private final JsonObject config;
        EntityManagerFactorySupplier(JsonObject config) {
            this.config = config;
        }

        @Override
        public EntityManagerFactory get() {
            JsonObject configUsed = config == null ? new JsonObject() : config;
            if (JPAService.JPA_PROVIDER_HIBERNATE.equalsIgnoreCase(configUsed.getString(CONFIG_JPA_PROVIDER, JPA_PROVIDER_JPA))) {
                return new HibernateService.HibernateSessionFactorySupplier(configUsed).get();
            } else if (JPAService.JPA_PROVIDER_JPA.equalsIgnoreCase(configUsed.getString(CONFIG_JPA_PROVIDER, JPA_PROVIDER_JPA))) {
                JsonObject persistentConfig = configUsed.getJsonObject(CONFIG_PERSISTENT, new JsonObject());
                return Persistence.createEntityManagerFactory(configUsed.getString(CONFIG_PERSISTENT_NAME, DEFAULT_SERVICE_NAME), persistentConfig.getMap());
            }
            logger.error("Configuration used: {}", configUsed.encodePrettily());
            throw new IllegalStateException("Wrong configuration, no supported JPA provider specified, please use another method to create the JPAService.");
        }
    }

    // holder implementation
    private class EntityManagerFactoryHolder implements Shareable {
        private ExecutorService exec;
        private AtomicInteger refCount = new AtomicInteger(1);
        private EntityManagerFactory emf;

        synchronized ExecutorService exec() {
            if (exec == null) {
                exec = new ThreadPoolExecutor(1, 1, 1000L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(),
                        (r -> new Thread(r, "vertx-open-entity-manager-thread")));
            }
            return exec;
        }

        void incRefCount() {
            refCount.incrementAndGet();
        }

        void close(Handler<AsyncResult<Void>> completionHandler) {
            synchronized (vertx) {
                if (refCount.decrementAndGet() == 0) {
                    Promise<Void> f1 = Promise.promise();
                    Promise<Void> f2 = Promise.promise();
                    if (completionHandler != null) {
                        CompositeFuture.all(f1.future(), f2.future()).<Void> map(f -> null).setHandler(completionHandler);
                    }
                    if (emf != null) {
                        vertx.executeBlocking(future -> {
                            try {
                                emf.close();
                                future.complete();
                            } catch (Exception e) {
                                future.fail(e);
                            }
                        }, f2);
                    } else {
                        f2.complete();
                    }
                    try {
                        if (exec != null) {
                            exec.shutdown();
                        }
                        if (map != null) {
                            map.remove(serviceName);
                            if (map.isEmpty()) {
                                map.close();
                            }
                        }
                        f1.complete();
                    } catch (Throwable t) {
                        f1.fail(t);
                    }
                } else {
                    if (completionHandler != null) {
                        completionHandler.handle(Future.succeededFuture());
                    }
                }
            }
        }
    }

    // This will close the session after the db operation
    private <R> Single<R> withEntityManager(Function<EntityManager, ? extends SingleSource<R>> func) {
        final AtomicReference<EntityManager> s = new AtomicReference<>(null);
        return Single.fromCallable(this.emfHolder.emf::createEntityManager)
                // get the session using internal Executor
                .subscribeOn(Schedulers.from(exec))
                // all db operations executed in vertx blocking pool
                .observeOn(RxHelper.blockingScheduler(vertx))
                .flatMap(em -> {
                    s.set(em);
                    return func.apply(em);
                })
                .doOnError(e -> {
                    // this error handling does not prevent error from propagation to the downstream
                    logger.error("Failed on the db operation", e);
                })
                .doAfterTerminate(() -> {
                    if (s.get() != null) {
                        s.get().close();
                    }
                })
                // after time consuming db operations, switch back to event loop to dispatch the events
                .observeOn(RxHelper.scheduler(vertx));
    }

    private <R> Single<R> withTransaction(Function<EntityManager, ? extends SingleSource<R>> func) {
        return withEntityManager((em) -> {
            EntityTransaction transaction = em.getTransaction();
            try {
                transaction.begin();
                // put persistent operations before creating next Single within the apply method
                SingleSource<R> single = func.apply(em);
                transaction.commit();
                return single;
            } catch (Exception e) {
                transaction.rollback();
                throw e;
            }
        }) ;
    }

    // This will close the session after the db operation
    private Completable withEntityManagerCompletable(Function<EntityManager, ? extends CompletableSource> func) {
        final AtomicReference<EntityManager> s = new AtomicReference<>(null);
        return Single.fromCallable(this.emfHolder.emf::createEntityManager)
                // get the session using internal Executor
                .subscribeOn(Schedulers.from(exec))
                // all db operations executed in vertx blocking pool
                .observeOn(RxHelper.blockingScheduler(vertx))
                // convert to Completable in case of no result is expected.
                .flatMapCompletable(em -> {
                    s.set(em);
                    return func.apply(em);
                })
                .doOnError(e -> {
                    // this error handling does not prevent error from propagation to the downstream
                    logger.error("Failed on the db operation", e);
                })
                .doAfterTerminate(() -> {
                    if (s.get() != null) {
                        s.get().close();
                    }
                })
                // after time consuming db operations, switch back to event loop to dispatch the events
                .observeOn(RxHelper.scheduler(vertx));
    }

    private Completable withTransactionCompletable(Function<EntityManager, ? extends CompletableSource> func) {
        return withEntityManagerCompletable((em) -> {
            EntityTransaction transaction = em.getTransaction();
            try {
                transaction.begin();
                // put persistent operations before creating next Single within the apply method
                CompletableSource single = func.apply(em);
                transaction.commit();
                return single;
            } catch (Exception e) {
                transaction.rollback();
                throw e;
            }
        });
    }

    private void setParams(Query query, JsonObject params) {
        if (params != null && !params.isEmpty()) {
            for (Map.Entry<String, Object> entry : params) {
                query.setParameter(entry.getKey(), entry.getValue());
            }
        }
    }

    /**
     * Converts the field value to JsonObject if possible in case of an entity type.
     * Return the value directly if it is a primitive type.
     *
     * @param val the single data which will be 
     * @return the JsonObject or the val itself in case of primitive types
     */
    private Object fieldData(Object val) {
        assert !val.getClass().isArray() : "val must not an array";
        Class<?> cls = val.getClass();
        if (cls.equals(String.class) || (val instanceof Number && !(val instanceof BigDecimal)) || cls.equals(Boolean.class)
                || cls.equals(Character.class) || cls.equals(CharSequence.class) || cls.equals(byte[].class)
                || cls.equals(Instant.class) || cls.isEnum()) {
            return val;
        }
        try {
            Method toJsonMethod = val.getClass().getMethod("toJson");
            return toJsonMethod.invoke(val);
        } catch (Exception e) {
            return JsonObject.mapFrom(val);
        }
    }

    /**
     * Converts data of one row in the ResultSet to one JsonObject.
     *
     * @param row the data of one row in the Result set
     * @param labels the labels used for the keys in the JsonObject
     * @return the JsonObject represents the data of the row
     */
    private JsonObject singleRowToJsonObject(Object row, List<String> labels) {
        JsonObject json = new JsonObject();
        if (row != null) {
            if (row.getClass().equals(JsonObject.class)) return (JsonObject)row;
            if (row.getClass().isArray()) {
                // examples: select name, description from project
                // labels are required in this case
                Object[] arrays = (Object[])row;
                if (arrays.length != labels.size()) {
                    throw new IllegalArgumentException("Labels are not valid, please check the length of searched fields.");
                }
                int i = 0;
                for (Object obj: arrays) {
                    json.put(labels.get(i++), fieldData(obj));
                }
            } else {
                // examples: select name from project
                // examples: select p from Project p
                Object data = fieldData(row);
                if (data instanceof JsonObject) {
                    json = (JsonObject)data;
                } else {
                    // labels are required in this case
                    if (labels.size() != 1) {
                        throw new IllegalArgumentException("A label must be provided for the result.");
                    }
                    json.put(labels.get(0), data);
                }
            }
        }
        return json;
    }

    // ===========================================================================================
    // ===================   Followings are methods to do the SQL query/execute   ================
    // ===========================================================================================

    @Override
    public JPAService queryName(String queryName, JsonObject params, Handler<AsyncResult<JsonArray>> resultHandler) {
        withEntityManager((em) -> {
            Query query = em.createNamedQuery(queryName);
            setParams(query, params);
            Stream<?> rsStream = query.getResultStream();
            List<Object> rowResult = rsStream.map(row -> {
                if (row.getClass().isArray()) {
                    throw new IllegalStateException("Entity type is expected in the ResultSet, but it is an array: " + row.getClass() + ", Please use withLabels variants of methods");
                }
                return singleRowToJsonObject(row, Collections.emptyList());
            }).collect(Collectors.toList());
            return Flowable.fromIterable(rowResult)
                    .collect(JsonArray::new, JsonArray::add);
        }).subscribe(SingleHelper.toObserver(resultHandler));
        return this;
    }

    @Override
    public JPAService queryNameWithLabels(String queryName, JsonObject params, List<String> labels,
            Handler<AsyncResult<JsonArray>> resultHandler) {
        withEntityManager((em) -> {
            Query query = em.createNamedQuery(queryName);
            setParams(query, params);
            Stream<?> rsStream = query.getResultStream();
            List<Object> rowResult = rsStream.map(row -> singleRowToJsonObject(row, labels)).collect(Collectors.toList());
            return Flowable.fromIterable(rowResult)
                    .collect(JsonArray::new, JsonArray::add);
        }).subscribe(SingleHelper.toObserver(resultHandler));
        return this;
    }

    @Override
    public JPAService queryJQL(String queryString, JsonObject params, Handler<AsyncResult<JsonArray>> resultHandler) {
        withEntityManager((em) -> {
            Query query = em.createQuery(queryString);
            setParams(query, params);
            Stream<?> rsStream = query.getResultStream();
            List<Object> rowResult = rsStream.map(row -> {
                if (row.getClass().isArray()) {
                    throw new IllegalStateException("Entity type is expected in the ResultSet, but it is an array: " + row.getClass());
                }
                return singleRowToJsonObject(row, Collections.emptyList());
            }).collect(Collectors.toList());
            return Flowable.fromIterable(rowResult)
                    .collect(JsonArray::new, JsonArray::add);
        }).subscribe(SingleHelper.toObserver(resultHandler));
        return this;
    }

    @Override
    public JPAService queryJQLWithLabels(String queryString, JsonObject params, List<String> labels,
            Handler<AsyncResult<JsonArray>> resultHandler) {
        withEntityManager((em) -> {
            Query query = em.createQuery(queryString);
            setParams(query, params);
            Stream<?> rsStream = query.getResultStream();
            List<Object> jsonResult = rsStream.map(row -> singleRowToJsonObject(row, labels)).collect(Collectors.toList());
            return Flowable.fromIterable(jsonResult)
                    .collect(JsonArray::new, JsonArray::add);
        }).subscribe(SingleHelper.toObserver(resultHandler));
        return null;
    }

    @Override
    public JPAService querySQLWithLabels(String sqlQueryString, JsonObject params, List<String> labels,
            Handler<AsyncResult<JsonArray>> resultHandler) {
        withEntityManager((em) -> {
            Query query = em.createNativeQuery(sqlQueryString);
            setParams(query, params);
            Stream<?> rsStream = query.getResultStream();
            List<Object> jsonResult = rsStream.map(row -> singleRowToJsonObject(row, labels)).collect(Collectors.toList());
            return Flowable.fromIterable(jsonResult)
                    .collect(JsonArray::new, JsonArray::add);
        }).subscribe(SingleHelper.toObserver(resultHandler));
        return null;
    }

    @Override
    public JPAService queryOneName(String queryName, JsonObject params,
            Handler<AsyncResult<JsonObject>> resultHandler) {
        withEntityManager((session) -> {
            Query query = session.createNamedQuery(queryName);
            setParams(query, params);
            List<?> result = query.getResultList();
            Object row = result.isEmpty() ? new JsonObject() : result.get(0);
            return Single.fromCallable(() -> singleRowToJsonObject(row, Collections.emptyList()));
        }).subscribe(SingleHelper.toObserver(resultHandler));
        return this;
    }

    @Override
    public JPAService queryOneNameWithLabels(String queryName, JsonObject params, List<String> labels,
            Handler<AsyncResult<JsonObject>> resultHandler) {
        withEntityManager((session) -> {
            Query query = session.createNamedQuery(queryName);
            setParams(query, params);
            List<?> result = query.getResultList();
            Object row = result.isEmpty() ? new JsonObject() : result.get(0);
            return Single.fromCallable(() -> singleRowToJsonObject(row, labels));
        }).subscribe(SingleHelper.toObserver(resultHandler));
        return this;
    }

    @Override
    public JPAService queryOneJQL(String queryString, JsonObject params, Handler<AsyncResult<JsonObject>> resultHandler) {
        withEntityManager((em) -> {
            Query query = em.createQuery(queryString);
            setParams(query, params);
            List<?> result = query.getResultList();
            Object row = result.isEmpty() ? new JsonObject() : result.get(0);
            return Single.fromCallable(() -> singleRowToJsonObject(row, Collections.emptyList()));
        }).subscribe(SingleHelper.toObserver(resultHandler));
        return this;
    }

    @Override
    public JPAService queryOneJQLWithLabels(String queryString, JsonObject params, List<String> labels,
            Handler<AsyncResult<JsonObject>> resultHandler) {
        withEntityManager((em) -> {
            Query query = em.createQuery(queryString);
            setParams(query, params);
            List<?> result = query.getResultList();
            Object row = result.isEmpty() ? new JsonObject() : result.get(0);
            return Single.fromCallable(() -> singleRowToJsonObject(row, labels));
        }).subscribe(SingleHelper.toObserver(resultHandler));
        return this;
    }

    @Override
    public JPAService findOne(String entityClassName, JsonObject primaryKey, Handler<AsyncResult<JsonObject>> resultHandler) {
        withEntityManager((em) -> {
            Class<?> entityClass = Class.forName(entityClassName);
            Object value = primaryKey.getValue("value");
            if (value == null) {
                throw new IllegalArgumentException("'value' is not defined in the PrimaryKey: " + primaryKey.toString());
            }
            Object entity = em.find(entityClass, value);
            return Single.fromCallable(() -> singleRowToJsonObject(entity, Collections.emptyList()));
        }).subscribe(SingleHelper.toObserver(resultHandler));
        return this;
    }

    @Override
    public JPAService save(String entityClassName, JsonObject json, Handler<AsyncResult<Void>> resultHandler) {
        withTransactionCompletable((em) -> {
            em.merge(json.mapTo(Class.forName(entityClassName)));
            return Completable.complete();
        }).subscribe(CompletableHelper.toObserver(resultHandler));
        return this;
    }

    @Override
    public JPAService remove(String entityClassName, JsonObject primaryKey, Handler<AsyncResult<Void>> resultHandler) {
        withTransactionCompletable((em) -> {
            Class<?> entityClass = Class.forName(entityClassName);
            Object value = primaryKey.getValue("value");
            if (value == null) {
                throw new IllegalArgumentException("'value' is not defined in the PrimaryKey: " + primaryKey.toString());
            }
            Object entity = em.find(entityClass, value);
            if (entity != null)
                em.remove(entity);
            return Completable.complete();
        }).subscribe(CompletableHelper.toObserver(resultHandler));
        return this;
    }

    @Override
    public JPAService executeQuery(String queryString, JsonObject params, Handler<AsyncResult<Integer>> resultHandler) {
        Objects.requireNonNull(queryString, "Query is required");
        final JsonObject parameters = params == null ? new JsonObject() : params;
        withTransaction((em) -> {
            Query query = em.createQuery(queryString);
            setParams(query, parameters);
            final int count = query.executeUpdate();
            return Single.just(count);
        }).subscribe(SingleHelper.toObserver(resultHandler));
        return this;
    }

    @Override
    public JPAService executeQueries(List<String> queries, JsonArray params, Handler<AsyncResult<Integer>> resultHandler) {
        Objects.requireNonNull(queries, "Queries are required");
        if (queries.isEmpty()) {
            throw new IllegalArgumentException("Queries are empty.");
        }
        final JsonArray paramsArray = params == null ? new JsonArray() : params;
        withTransaction((em) -> {
            int count = 0;
            int i = 0;
            for (String queryString: queries) {
                Query query = em.createQuery(queryString);
                setParams(query, paramsArray.getJsonObject(i++));
                count =+ query.executeUpdate();
            }
            return Single.just(count);
        }).subscribe(SingleHelper.toObserver(resultHandler));
        return this;
    }

}
