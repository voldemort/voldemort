/*
 * Copyright 2013 LinkedIn, Inc
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

package voldemort.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import voldemort.VoldemortException;

/**
 * Two dimensional counter. Currently used to count moves in a rebalancing plan.
 * The toString method is tailored to this use case. The structure may be
 * generally useful though, the 'indices' is hard-coded to be an Integer at this
 * time.
 * 
 */
public class MoveMap {

    // TODO: (refactor) create a voldemort.utils.rebalance package and move this
    // class (and others) to it.

    final TreeSet<Integer> idKeySet;
    final Map<Integer, Map<Integer, Integer>> fromToMoves;

    public MoveMap(Set<Integer> ids) {
        idKeySet = new TreeSet<Integer>(ids);
        fromToMoves = new HashMap<Integer, Map<Integer, Integer>>();

        for(Integer fromId: ids) {
            fromToMoves.put(fromId, new HashMap<Integer, Integer>());
            for(Integer toId: ids) {
                fromToMoves.get(fromId).put(toId, new Integer(0));
            }
        }
    }

    public int increment(int fromId, int toId) {
        return this.add(fromId, toId, 1);
    }

    public int add(int fromId, int toId, int amount) {
        int moves = fromToMoves.get(fromId).get(toId);
        moves += amount;
        fromToMoves.get(fromId).put(toId, moves);
        return moves;
    }

    public int get(int fromId, int toId) {
        return fromToMoves.get(fromId).get(toId);
    }

    public void add(MoveMap rhs) {
        if(!idKeySet.containsAll(rhs.idKeySet) || !rhs.idKeySet.containsAll(idKeySet)) {
            throw new VoldemortException("MoveMap objects cannot be added! They have incompatible id sets ("
                                         + idKeySet + " vs. " + rhs.idKeySet + ").");
        }
        for(Integer fromId: idKeySet) {
            for(Integer toId: idKeySet) {
                int moves = get(fromId, toId) + rhs.get(fromId, toId);
                fromToMoves.get(fromId).put(toId, moves);
            }
        }
    }

    public Map<Integer, Integer> groupByFrom() {
        Map<Integer, Integer> groupByFrom = new HashMap<Integer, Integer>();
        for(Integer id: idKeySet) {
            groupByFrom.put(id, 0);
        }
        for(Integer fromId: idKeySet) {
            for(Integer toId: idKeySet) {
                int moves = get(fromId, toId) + groupByFrom.get(fromId);
                groupByFrom.put(fromId, moves);
            }
        }
        return groupByFrom;
    }

    public Map<Integer, Integer> groupByTo() {
        Map<Integer, Integer> groupByTo = new HashMap<Integer, Integer>();
        for(Integer id: idKeySet) {
            groupByTo.put(id, 0);
        }
        for(Integer fromId: idKeySet) {
            for(Integer toId: idKeySet) {
                int moves = get(fromId, toId) + groupByTo.get(toId);
                groupByTo.put(toId, moves);
            }
        }
        return groupByTo;
    }

    public String toFlowString() {
        StringBuilder sb = new StringBuilder();

        Map<Integer, Integer> froms = groupByFrom();
        Map<Integer, Integer> tos = groupByTo();

        for(Integer id: idKeySet) {
            sb.append(String.format("%6d", tos.get(id)) + " -> (" + String.format("%4d", id)
                      + ") -> " + String.format("%6d", froms.get(id)) + "\n");
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        for(Integer fromId: idKeySet) {
            for(Integer toId: idKeySet) {
                sb.append("(" + String.format("%4d", fromId) + ") -> ("
                          + String.format("%4d", toId) + ") = "
                          + String.format("%6d", fromToMoves.get(fromId).get(toId)) + "\n");
            }
        }
        return sb.toString();
    }
}
