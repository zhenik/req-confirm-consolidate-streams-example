package ru.zhenik.kafka.example.streams;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("status")
@Produces(MediaType.APPLICATION_JSON)
public class StatusResources {

  private StatusStateStreamProcessor repository;

  public StatusResources(StatusStateStreamProcessor repository) { this.repository = repository; }

  @GET
  public Response getPatterns() {
    return Response.ok(repository.getValues()).build();
  }

}
