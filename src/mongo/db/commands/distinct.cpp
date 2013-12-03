// distinct.cpp

/**
*    Copyright (C) 2012 10gen Inc.
*
*    This program is free software: you can redistribute it and/or  modify
*    it under the terms of the GNU Affero General Public License, version 3,
*    as published by the Free Software Foundation.
*
*    This program is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
*    GNU Affero General Public License for more details.
*
*    You should have received a copy of the GNU Affero General Public License
*    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

#include <string>
#include <vector>

#include "mongo/db/auth/action_set.h"
#include "mongo/db/auth/action_type.h"
#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/auth/privilege.h"
#include "mongo/db/clientcursor.h"
#include "mongo/db/collection.h"
#include "mongo/db/commands.h"
#include "mongo/db/curop.h"
#include "mongo/db/instance.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/kill_current_op.h"
#include "mongo/db/query/get_runner.h"
#include "mongo/db/query/query_planner_common.h"
#include "mongo/db/query/type_explain.h"
#include "mongo/util/timer.h"

namespace mongo {

    class DistinctCommand : public Command {
    public:
        DistinctCommand() : QueryCommand("distinct") {}
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::find);
            out->push_back(Privilege(parseNs(dbname, cmdObj), actions));
        }
        virtual void help( stringstream &help ) const {
            help << "{ distinct : 'collection name' , key : 'a.b' , query : {} }";
        }

        bool run(const string& dbname, BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool fromRepl ) {
            Timer t;
            string ns = dbname + '.' + cmdObj.firstElement().valuestr();

            string key = cmdObj["key"].valuestrsafe();
            BSONObj keyPattern = BSON( key << 1 );

            BSONObj query = getQuery( cmdObj );

            int bufSize = BSONObjMaxUserSize - 4096;
            BufBuilder bb( bufSize );
            char * start = bb.buf();

            BSONArrayBuilder arr( bb );
            BSONElementSet values;

            long long nscanned = 0; // locations looked at
            long long nscannedObjects = 0; // full objects looked at
            long long n = 0; // matches
            MatchDetails md;

            Collection *cl = getCollection( ns );

            if ( ! cl ) {
                result.appendArray( "values" , BSONObj() );
                result.append( "stats" , BSON( "n" << 0 << "nscanned" << 0 << "nscannedObjects" << 0 ) );
                return true;
            }

            CanonicalQuery* cq;
            // XXX: project out just the field we're distinct-ing.  May be covered...
            if (!CanonicalQuery::canonicalize(ns, query, &cq).isOK()) {
                uasserted(17215, "Can't canonicalize query " + query.toString());
                return 0;
            }

            Runner* rawRunner;
            if (!getRunner(cq, &rawRunner).isOK()) {
                uasserted(17216, "Can't get runner for query " + query.toString());
                return 0;
            }

            auto_ptr<Runner> runner(rawRunner);
            auto_ptr<DeregisterEvenIfUnderlyingCodeThrows> safety;
            ClientCursor::registerRunner(runner.get());
            runner->setYieldPolicy(Runner::YIELD_AUTO);
            safety.reset(new DeregisterEvenIfUnderlyingCodeThrows(runner.get()));

            BSONObj obj;
            Runner::RunnerState state;
            while (Runner::RUNNER_ADVANCED == (state = runner->getNext(&obj, NULL))) {
                BSONElementSet elts;
                obj.getFieldsDotted(key, elts);

                for (BSONElementSet::iterator it = elts.begin(); it != elts.end(); ++it) {
                    BSONElement elt = *it;
                    if (values.count(elt)) { continue; }
                    int currentBufPos = bb.len();

                    uassert(17217, "distinct too big, 16mb cap",
                            (currentBufPos + elt.size() + 1024) < bufSize);

                    arr.append(elt);
                    BSONElement x(start + currentBufPos);
                    values.insert(x);
                }
            }
            TypeExplain* bareExplain;
            Status res = runner->getExplainPlan(&bareExplain);
            if (res.isOK()) {
                auto_ptr<TypeExplain> explain(bareExplain);
                if (explain->isCursorSet()) {
                    cursorName = explain->getCursor();
                }
                n = explain->getN();
                nscanned = explain->getNScanned();
                nscannedObjects = explain->getNScannedObjects();
            }

            verify( start == bb.buf() );

            result.appendArray( "values" , arr.done() );

            {
                BSONObjBuilder b;
                b.appendNumber( "n" , n );
                b.appendNumber( "nscanned" , nscanned );
                b.appendNumber( "nscannedObjects" , nscannedObjects );
                b.appendNumber( "timems" , t.millis() );
                b.append( "cursor" , cursorName );
                result.append( "stats" , b.obj() );
            }

            return true;
        }
    } distinctCmd;

}  // namespace mongo
