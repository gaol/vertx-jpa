package com.spidercoding.vertx.jpa.model;

import java.util.List;
import java.util.Objects;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.OneToMany;
import javax.persistence.Table;

import io.vertx.core.json.JsonObject;

@Entity
@Table(name = "User")
@NamedQueries({
    @NamedQuery(name = "getByUserName", query = "SELECT u FROM User u WHERE u.username = :username")
})
public class User {

    @Id @GeneratedValue(strategy = GenerationType.AUTO)
    private long id = -1L;

    private String username;

    @OneToMany(mappedBy = "user")
    private List<Project> projects;

    public JsonObject toJson() {
        JsonObject json = new JsonObject();
        json.put("id", id);
        if (this.username != null) json.put("username", this.username);
        return json;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public List<Project> getProjects() {
        return projects;
    }

    public void setProjects(List<Project> projects) {
        this.projects = projects;
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
        User other = (User) obj;
        return id == other.id;
    }
    
}
