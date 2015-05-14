package voldemort.store.readonly.hooks.http;

import com.google.common.collect.Lists;
import voldemort.store.readonly.hooks.AbstractBuildAndPushHook;
import voldemort.store.readonly.hooks.BuildAndPushStatus;
import voldemort.utils.Props;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class HttpHook extends AbstractBuildAndPushHook {

    // Config keys
    private final String URL_TO_CALL = configKeyPrefix + "url";
    private final String EXECUTOR_THREADS = configKeyPrefix + "num-threads";

    // Config values
    private String urlToCall = null;

    // Execution
    private ExecutorService executorService;
    private final List<Future> httpFutureResults = Lists.newArrayList();
    private final List<BuildAndPushStatus> terminationStatuses = Lists.newArrayList(
            BuildAndPushStatus.FINISHED,
            BuildAndPushStatus.CANCELLED,
            BuildAndPushStatus.FAILED);
    private final List<BuildAndPushStatus> statusesToCallHookFor = getStatusListToCallHookFor();
    private AtomicBoolean running = new AtomicBoolean(true);

    @Override
    public void init(Props properties) throws Exception {
        this.urlToCall = properties.getString(URL_TO_CALL);
        int numThreads = properties.getInt(EXECUTOR_THREADS, 1);
        this.executorService = Executors.newFixedThreadPool(numThreads);
    }

    @Override
    public synchronized void invoke(BuildAndPushStatus buildAndPushStatus, String details) {
        if (running.get() == true) {
            if (statusesToCallHookFor.contains(buildAndPushStatus)) {
                httpFutureResults.add(this.executorService.submit(new HttpHookRunnable(
                        getName(),
                        log,
                        getUrlToCall(buildAndPushStatus, details),
                        getHttpMethod(buildAndPushStatus, details),
                        getContentType(buildAndPushStatus, details),
                        getRequestBody(buildAndPushStatus, details))));
            }
            if (terminationStatuses.contains(buildAndPushStatus)) {
                cleanUp();
            }
        } else {
            log.error("HttpHook [" + getName() + "] was invoked after having already terminated! " +
                    "Status: " + buildAndPushStatus + ", details: " + details);
        }
    }

    /**
     * Override this function if you need another http method than POST.
     *
     * @param buildAndPushStatus
     * @return the method to use in the HTTP request
     */
    protected HttpMethod getHttpMethod(BuildAndPushStatus buildAndPushStatus, String details) {
        return HttpMethod.POST;
    }

    /**
     * Override this function if you need a Content-type header specified.
     *
     * The default implementation (returning null) will cause no Content-type header
     * at all to be used.
     *
     * @param buildAndPushStatus
     * @return the content-type to use in the HTTP request, can be null
     */
    protected String getContentType(BuildAndPushStatus buildAndPushStatus, String details) {
        return null;
    }

    /**
     * Override this function if you need a custom URL specified.
     *
     * The default implementation will return the parameter that was set via the URL_TO_CALL
     * config property. If you choose to override, you can still get the value of URL_TO_CALL
     * by invoking super.getUrlToCall (for example if you need to append some dynamic GET
     * parameters at the end).
     *
     * @param buildAndPushStatus
     * @return the content-type to use in the HTTP request, can be null
     */
    protected String getUrlToCall(BuildAndPushStatus buildAndPushStatus, String details) {
        return urlToCall;
    }

    /**
     * Concrete classes must implement this to declare which {@link BuildAndPushStatus}
     * the hook should send an HTTP request for.
     *
     * @return a list of {@link BuildAndPushStatus} to act on
     */
    protected abstract List<BuildAndPushStatus> getStatusListToCallHookFor();

    /**
     * Concrete classes must implement this to provide a request body to include in the
     * HTTP request. Can return null, in which case, no request body will be included
     * (for example, if the HTTP method is GET).
     *
     * @param buildAndPushStatus
     * @return the request body to include in the HTTP request
     */
    protected abstract String getRequestBody(BuildAndPushStatus buildAndPushStatus, String details);

    private synchronized void cleanUp() {
        if (running.get() == true) {
            for (Future result : httpFutureResults) {
                try {
                    result.get();
                } catch (Exception e) {
                    this.log.error("Exception while getting the result of the " +
                            getName() + "'s HTTP request...", e);
                }
            }
            this.executorService.shutdown();
            running.set(false);
        }
    }
}
