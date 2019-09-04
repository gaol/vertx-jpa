package io.github.gaol.vertx.ext.jpa;

import java.util.Properties;
import java.util.function.Supplier;

import javax.persistence.EntityManagerFactory;

import org.hibernate.boot.MetadataSources;
import org.hibernate.boot.registry.BootstrapServiceRegistryBuilder;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.service.ServiceRegistry;

import com.github.fluent.hibernate.cfg.scanner.EntityScanner;

import io.reactivex.Single;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * HibernateService creates a JPAService with Hibernate as the JPA provider.
 */
public class HibernateService {

    public static final String CONFIG_SCANN_PACKAGES = "hibernate.scanning.packages";
    public static final String CONFIG_ENTITY_CLASSES = "hibernate.entity.classes";

    public static Single<JPAService> rxCreate(Vertx vertx, String serviceName, JsonObject config) {
        return JPAService.rxCreate(vertx, serviceName, config, new HibernateSessionFactorySupplier(config));
    }

    static class HibernateSessionFactorySupplier implements Supplier<EntityManagerFactory> {
        private final JsonObject config;

        HibernateSessionFactorySupplier(JsonObject config) {
            this.config = config;
        }

        @Override
        public EntityManagerFactory get() {
            ServiceRegistry standardRegistry = new StandardServiceRegistryBuilder(new BootstrapServiceRegistryBuilder().build()).build();
            MetadataSources sources = new MetadataSources(standardRegistry);
            Configuration configuration = new Configuration(sources);

            Properties props = new Properties();
            props.putAll(config.getJsonObject(JPAService.CONFIG_PERSISTENT, new JsonObject()).getMap());
            configuration.setProperties(props);

            configuration.addAttributeConverter(JsonObjectAttributeConverter.class, true);

            config.getJsonArray(CONFIG_ENTITY_CLASSES, new JsonArray()).forEach(cls -> {
                try {
                    configuration.addAnnotatedClass(Class.forName(cls.toString()));
                } catch (ClassNotFoundException e) {
                    throw new IllegalStateException("Could not load class: " + cls.toString(), e);
                }
            });
            JsonArray scanPackages = config.getJsonArray(CONFIG_SCANN_PACKAGES, new JsonArray());
            String[] packages = new String[scanPackages.size()];
            int i = 0;
            for (Object pkg: scanPackages) {
                packages[i++] = pkg.toString();
            }
            EntityScanner.scanPackages(packages).addTo(configuration);

            return configuration.buildSessionFactory();
        }

    }

}
