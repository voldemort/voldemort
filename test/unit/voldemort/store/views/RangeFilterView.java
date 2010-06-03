package voldemort.store.views;

import java.util.ArrayList;
import java.util.List;

import voldemort.store.Store;
import voldemort.versioning.Versioned;

/**
 * @author nnarkhed
 * 
 */
public class RangeFilterView implements View<Integer, List<Integer>, List<Integer>, List<Integer>> {

    public List<Integer> storeToView(Store<Integer, List<Integer>, List<Integer>> targetStore,
                                     Integer k,
                                     List<Integer> s,
                                     List<Integer> t) throws UnsupportedViewOperationException {

        List<Integer> filteredValues = new ArrayList<Integer>();
        // t should be a list of 2 values - min and max
        if(t != null) {
            if(t.size() != 2)
                throw new UnsupportedViewOperationException("t is supposed to be a list of 2 values - min and max");
            for(Integer val: s) {
                if((val.compareTo(t.get(0)) >= 0) && (val.compareTo(t.get(1)) <= 0)) {
                    filteredValues.add(val);
                }
            }
        } else
            filteredValues = s;
        return filteredValues;
    }

    public List<Integer> viewToStore(Store<Integer, List<Integer>, List<Integer>> targetStore,
                                     Integer k,
                                     List<Integer> v,
                                     List<Integer> t) throws UnsupportedViewOperationException {
        List<Integer> filteredValues = new ArrayList<Integer>();
        // t should be a list of 2 values - min and max
        if(t != null) {
            if(t.size() != 2)
                throw new UnsupportedViewOperationException("t is supposed to be a list of 2 values - min and max");
            for(Integer val: v) {
                if((val.compareTo(t.get(0)) >= 0) && (val.compareTo(t.get(1)) <= 0)) {
                    filteredValues.add(val);
                }
            }
        } else {
            filteredValues = v;
        }
        List<Versioned<List<Integer>>> prevValues = targetStore.get(k, null);
        if(prevValues.size() == 0)
            return filteredValues;
        List<Integer> newValues = prevValues.get(0).getValue();
        newValues.addAll(filteredValues);
        return newValues;
    }

}
