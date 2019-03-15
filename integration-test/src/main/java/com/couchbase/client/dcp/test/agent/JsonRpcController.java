package com.couchbase.client.dcp.test.agent;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.therapi.core.MethodRegistry;
import com.github.therapi.jsonrpc.DefaultExceptionTranslator;
import com.github.therapi.jsonrpc.JsonRpcDispatcher;
import com.github.therapi.jsonrpc.JsonRpcError;
import com.github.therapi.jsonrpc.web.AbstractSpringJsonRpcController;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;

import java.util.HashMap;
import java.util.Map;


@Controller
public class JsonRpcController extends AbstractSpringJsonRpcController {
  private ObjectMapper objectMapper;

  @Autowired
  @Qualifier("jsonRpcObjectMapper")
  public void setObjectMapper(ObjectMapper objectMapper) {
    this.objectMapper = objectMapper;
  }

  @Override
  public ObjectMapper getObjectMapper() {
    return objectMapper;
  }

  @Override
  protected JsonRpcDispatcher newJsonRpcDispatcher(MethodRegistry registry) {
    return JsonRpcDispatcher.builder(registry)
        .exceptionTranslator(new DefaultExceptionTranslator() {
          @Override
          protected JsonRpcError translateCustom(Throwable t) {
            Map<String, Object> data = new HashMap<>();
            data.put("exceptionClass", t.getClass().getSimpleName());
            data.put("exceptionMessage", t.getMessage());
            JsonRpcError error = new JsonRpcError(2440, "Couchbase Error");
            error.setData(data);
            return error;
          }
        })
        .build();
  }
}
