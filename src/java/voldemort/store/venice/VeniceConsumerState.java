package voldemort.store.venice;

/**
 * An enum which stores the possible states for a Venice Consumer.
 * TODO: Possibly leverage Helix for these states and their transitions.
 * This is unlikely to happen in voldemort but should be done in Venice
 */
public enum VeniceConsumerState {
    RUNNING, PAUSED, CATCHING_UP
}
