package com.spidercoding.vertx.jpa.model;

import java.util.Objects;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.ManyToOne;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Table;

import io.vertx.core.json.JsonObject;

@Entity
@Table(name = "Project")
@NamedQueries({ 
        @NamedQuery(name = "getProjectsByUser", query = "SELECT p FROM Project p WHERE p.user.id = :uid"),
        @NamedQuery(name = "getProjectNamesByUser", query = "SELECT p.id, p.name FROM Project p WHERE p.user.id = :uid")
     })
public class Project {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private long id;

    private String name;

    @ManyToOne
    private User user;

    public JsonObject toJson() {
        JsonObject json = new JsonObject();
        json.put("id", id);
        if (this.name != null) json.put("name", this.name);
        if (this.user != null) json.put("user", user.toJson());
        return json;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public User getUser() {
        return user;
    }

    public void setUser(User user) {
        this.user = user;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Project other = (Project) obj;
        return id == other.id;
    }

}
