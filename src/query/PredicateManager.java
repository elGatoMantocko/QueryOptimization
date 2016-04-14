package query;

import global.AttrOperator;
import global.AttrType;
import relop.Iterator;
import relop.Predicate;
import relop.Schema;
import relop.Selection;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Created by david on 4/14/16.
 */
public class PredicateManager {
    private final Predicate[][] mPredicates;
    private final List<Predicate[]> mPredicatesList;

    public PredicateManager(Predicate[][] preds) {
        this.mPredicatesList = new ArrayList<>();
        this.mPredicatesList.addAll(Arrays.asList(preds));

        this.mPredicates = preds;

    }

    public Predicate[] popApplicableJoinPredicate(Iterator left, Iterator right) {
        Schema joinshadow = Schema.join(left.getSchema(), right.getSchema());
        int bestPredScore = Integer.MIN_VALUE;
        Predicate[] predToJoinOn = null;
        predToJoinOn = getJoinPredicate(joinshadow, bestPredScore);

        if(predToJoinOn != null)
            mPredicatesList.remove(predToJoinOn);

        return predToJoinOn;
    }

    public Collection<Predicate[]> popApplicablePredicates(Iterator iterator) {
        // save the schema for this table
        Schema tableSchema = iterator.getSchema();
        ArrayList<Predicate[]> applicablePreds = new ArrayList<>();

        for (Predicate[] candidate : mPredicatesList) {
            boolean canPushSelect = true;

            for (Predicate p : candidate) {
                canPushSelect = canPushSelect && p.validate(tableSchema);
            }

            if (canPushSelect) {
                applicablePreds.add(candidate);
            }
        }

        mPredicatesList.removeAll(applicablePreds);
        return applicablePreds;
    }

    private Predicate[] getJoinPredicate(Schema joinshadow, int bestPredScore) {
        Predicate[] predToJoinOn = null;
        //find the predicate to join on
        for (Predicate[] candidate : mPredicatesList) {
            boolean validCandidate = true;
            int score = 0;

            for (Predicate p : candidate) {
                validCandidate = validCandidate && p.validate(joinshadow);
                if (p.getOper() == AttrOperator.EQ && p.getLtype() == AttrType.COLNAME && p.getRtype() == AttrType.COLNAME) {
                    score++;
                }
            }
            if (validCandidate && score > bestPredScore) {
                predToJoinOn = candidate;
                bestPredScore = score;
            }
        }
        return predToJoinOn;
    }
}
