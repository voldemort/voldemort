package voldemort.store.readonly.hooks.http;

/**
 * Enum of valid HTTP methods usable in the {@link HttpHook}
 */
public enum HttpMethod {
  GET,
  POST,
  HEAD,
  OPTIONS,
  PUT,
  DELETE,
  TRACE;
}
