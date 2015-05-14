package voldemort.store.readonly.hooks;

import voldemort.utils.Props;

/**
 * This can be implemented in order to act on certain stages of the Build and Push job.
 *
 * Implementations of this interface must have a no-arguments constructor.
 *
 * All hooks registered via the job's config will be instantiated by reflection, and
 * then provided the job's properties via the {@link #init(Props)}
 * function. Afterward, the {@link #invoke(BuildAndPushStatus, String)} function will
 * be called at various stages of the job, within a try/catch (i.e.: the hooks are
 * executed on a best effort basis and are not meant to be allowed to fail the job).
 */
public interface BuildAndPushHook {
  /**
   * A hook name, for logging purposes.
   *
   * @return a human-readable name for the hook.
   */
  String getName();

  /**
   * Hooks can inspect the job's regular config params as well as look for config params
   * that they need for their own operation. It is strongly recommended to name-space
   * config params that are hook-specific, in order to avoid name-clashes, according to
   * the following convention: "hooks." followed by the hook's FQCN, followed by a config
   * name, for example:
   *
   * hooks.com.company.fully.qualified.class.name.of.TheHook.some-config-name=some-config-value
   *
   * If this function throws an exception, its {@link #invoke(BuildAndPushStatus, String)}
   * function will not be called during the job.
   *
   * @param props of the Build and Push job.
   */
  void init(Props props) throws Exception;

  /**
   * This is called by the Build and Push job at various stages during the life-cycle of the job.
   *
   * It is called within a try/catch and will not allow the job to fail even if it throws.
   *
   * @param status an instance of the BuildAndPushStatus enum
   * @param details Can be null. Not all stages have details associated with them.
   */
  void invoke(BuildAndPushStatus status, String details);
}