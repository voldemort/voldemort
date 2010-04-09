/*
 * Copyright 2010 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package voldemort.store.routed;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import voldemort.store.routed.StateMachine.Operation;

public abstract class StateData {

    private final Operation operation;

    private Object results;

    private final List<Exception> failures;

    private final AtomicInteger attempts;

    private final AtomicInteger completed;

    private RuntimeException fatalError;

    public StateData(Operation operation) {
        this.operation = operation;
        this.failures = Collections.synchronizedList(new LinkedList<Exception>());
        this.attempts = new AtomicInteger(0);
        this.completed = new AtomicInteger(0);
    }

    public Operation getOperation() {
        return operation;
    }

    @SuppressWarnings("unchecked")
    public <T> T get() {
        if(fatalError != null)
            throw fatalError;

        return (T) results;
    }

    public <T> void setResults(T results) {
        this.results = results;
    }

    public RuntimeException getFatalError() {
        return fatalError;
    }

    public void setFatalError(RuntimeException fatalError) {
        this.fatalError = fatalError;
    }

    public List<Exception> getFailures() {
        return failures;
    }

    public void recordFailure(Exception e) {
        this.failures.add(e);
    }

    public int getAttempts() {
        return attempts.get();
    }

    public void setAttempts(int attempts) {
        this.attempts.set(attempts);
    }

    public int getCompleted() {
        return completed.get();
    }

    public void incrementCompleted() {
        completed.incrementAndGet();
    }

}
