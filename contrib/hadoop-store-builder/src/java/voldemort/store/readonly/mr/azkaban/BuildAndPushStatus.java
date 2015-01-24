package voldemort.store.readonly.mr.azkaban;

/**
 * This enum describes the various stages a Build and Push can be in.
 */
public enum BuildAndPushStatus {
  // The "happy path" status:
  STARTING,
  BUILDING,
  PUSHING,
  FINISHED,
  HEARTBEAT, // N.B.: The heartbeat is emitted periodically during any of the stages...
  // The "not-so-happy path" status:
  CANCELLED,
  FAILED;
}