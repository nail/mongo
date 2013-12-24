// delete.cpp

#include "mongo/db/clientcursor.h"
#include "mongo/db/namespace_details.h"
#include "mongo/db/query/get_runner.h"
#include "mongo/db/query/query_planner_common.h"
#include "mongo/db/repl/oplog.h"
#include "mongo/db/structure/collection.h"


namespace mongo {

    /* ns:      namespace, e.g. <database>.<collection>
       pattern: the "where" clause / criteria
       justOne: stop after 1 match
       god:     allow access to system namespaces, and don't yield
    */
    long long deleteObjects(const StringData& ns, BSONObj pattern, bool justOne, bool logop, bool god) {
        if (!god) {
            if (ns.find( ".system.") != string::npos) {
                // note a delete from system.indexes would corrupt the db if done here, as there are
                // pointers into those objects in NamespaceDetails.
                uassert(12050, "cannot delete from system namespace", legalClientSystemNS( ns, true ) );
            }

        long long nDeleted = 0;
        for (shared_ptr<Cursor> c(Cursor::make(cl, i, newMin, newMax, maxInclusive, 1));
             c->ok(); c->advance()) {
            const BSONObj pk = c->currPK();
            const BSONObj obj = c->current();
            OplogHelpers::logDelete(ns.c_str(), obj, fromMigrate);
            deleteOneObject(cl, pk, obj, flags);
            nDeleted++;
        }
        return nDeleted;
    }

        Collection* collection = currentClient.get()->database()->getCollection(ns);
        if (NULL == collection) {
            return 0;
        }

        uassert(10101,
                str::stream() << "can't remove from a capped collection: " << ns,
                !collection->isCapped());

        string nsForLogOp = ns.toString(); // XXX-ERH

        BSONObj obj;
        BSONObj pk = cl->getSimplePKFromQuery(pattern);

        CanonicalQuery* cq;
        if (!CanonicalQuery::canonicalize(ns.toString(), pattern, &cq).isOK()) {
            uasserted(17218, "Can't canonicalize query " + pattern.toString());
            return 0;
        }

        bool canYield = !god && !QueryPlannerCommon::hasNode(cq->root(), MatchExpression::ATOMIC);

        Runner* rawRunner;
        if (!getRunner(cq, &rawRunner).isOK()) {
            uasserted(17219, "Can't get runner for query " + pattern.toString());
            return 0;
        }

        auto_ptr<Runner> runner(rawRunner);
        auto_ptr<DeregisterEvenIfUnderlyingCodeThrows> safety;

        if (canYield) {
            ClientCursor::registerRunner(runner.get());
            runner->setYieldPolicy(Runner::YIELD_AUTO);
            safety.reset(new DeregisterEvenIfUnderlyingCodeThrows(runner.get()));
        }

        DiskLoc rloc;
        Runner::RunnerState state;
        while (Runner::RUNNER_ADVANCED == (state = runner->getNext(NULL, &rloc))) {

            BSONObj toDelete;

            // XXX: do we want to buffer docs and delete them in a group rather than
            // saving/restoring state repeatedly?
            runner->saveState();
            collection->deleteDocument(rloc, false, false, logop ? &toDelete : NULL );
            runner->restoreState();

            nDeleted++;

            if (logop) {
                if ( toDelete.isEmpty() ) {
                    problem() << "deleted object without id, not logging" << endl;
                }
                else {
                    bool replJustOne = true;
                    logOp("d", nsForLogOp.c_str(), toDelete, 0, &replJustOne);
                }
            }

            if (justOne) {
                break;
            }

            if (!god) {
                getDur().commitIfNeeded();
            }

            if (debug && god && nDeleted == 100) {
                log() << "warning high number of deletes with god=true "
                      << " which could use significant memory b/c we don't commit journal";
            }
        }

        return nDeleted;
    }
}
