package io.github.gaol.vertx.ext.jpa;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.github.gaol.vertx.ext.jpa.model.Project;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class JPAServiceTest {

    private static final String ADDRESS = "vertx.ext.jpa.service.address.test";

    private Vertx vertx;

    private JsonObject jpaVerticleConfig() {
        return new JsonObject()
                .put(JPAService.CONFIG_PERSISTENT, new JsonObject()
                        .put("hibernate.connection.driver_class", "org.h2.Driver")
                        .put("hibernate.dialect", "H2")
                        .put("hibernate.connection.url", "jdbc:h2:mem:jpa-test-db;DB_CLOSE_ON_EXIT=FALSE;DB_CLOSE_DELAY=-1")
                        .put("hibernate.connection.username", "sa")
                        .put("hibernate.connection.password", "sa")
                        .put("hibernate.show_sql", "true")
                        .put("hibernate.hbm2ddl.auto", "create-drop")
                        .put("hibernate.c3p0.min_size", "10")
                        .put("hibernate.c3p0.initialPoolSize", "30")
                        .put("hibernate.c3p0.max_size", "50")
                        .put("hibernate.hbm2ddl.import_files", "META-INF/test.sql")
                )
                .put(JPAVerticle.CONFIG_JPA_SERVICE_NAME, "vertx.ext.jpa.service.test")
                .put(JPAService.CONFIG_JPA_PROVIDER, JPAService.JPA_PROVIDER_HIBERNATE)
                .put(JPAVerticle.CONFIG_JPA_SERVICE_ADDRESS, ADDRESS)
                .put(HibernateService.CONFIG_SCANN_PACKAGES, new JsonArray().add(Project.class.getPackage().getName()))
                ;
    }

    @Before
    public void setUp(TestContext tc) {
      vertx = Vertx.vertx();
      vertx.deployVerticle(JPAVerticle.class.getName(), new DeploymentOptions().setConfig(jpaVerticleConfig()), tc.asyncAssertSuccess());
    }

    @After
    public void tearDown(TestContext tc) {
      vertx.close(tc.asyncAssertSuccess());
    }

    @Test
    public void testQueryName(TestContext tc) {
      Async async = tc.async();
      JPAService
          .createProxy(vertx, ADDRESS)
              .rxQueryName("getProjectsByUser", new JsonObject().put("uid", 1L))
          .subscribe((r, e) -> {
              if (e != null) {
                  tc.fail(e);
              } else {
                  tc.assertEquals(3, r.size());
                  async.complete();
              }
          });
    }

    @Test
    public void testQuerySQL(TestContext tc) {
      Async async = tc.async();
      List<String> labels = new ArrayList<>();
      labels.add("UserName");
      String sql = "SELECT u.username from User u INNER JOIN Project p ON p.user_id = u.id AND p.id = :pid";
      JPAService
          .createProxy(vertx, ADDRESS)
              .rxQuerySQLWithLabels(sql, new JsonObject().put("pid", 1L), labels)
          .subscribe((r, e) -> {
              if (e != null) {
                  tc.fail(e);
              } else {
                  tc.assertEquals(1, r.size());
                  tc.assertEquals("leo", r.getJsonObject(0).getString("UserName"));
                  async.complete();
              }
          });
    }

}
