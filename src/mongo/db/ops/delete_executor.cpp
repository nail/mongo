/**
 *    Copyright (C) 2014 MongoDB Inc.
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
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#include "mongo/platform/basic.h"

#include "mongo/db/ops/delete_executor.h"

#include "mongo/db/collection.h"
#include "mongo/db/client.h"
#include "mongo/db/clientcursor.h"
#include "mongo/db/curop.h"
#include "mongo/db/oplog.h"
#include "mongo/db/oplog_helpers.h"
#include "mongo/db/ops/delete.h"
#include "mongo/db/ops/delete_request.h"
#include "mongo/db/ops/query.h"
#include "mongo/db/queryutil.h"
#include "mongo/db/query_optimizer.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/mongoutils/str.h"
#include "mongo/util/stacktrace.h"


namespace mongo {

    DeleteExecutor::DeleteExecutor(const DeleteRequest* request) :
        _request(request),
        _canonicalQuery(),
        _isQueryParsed(false) {
    }

    DeleteExecutor::~DeleteExecutor() {}

    Status DeleteExecutor::prepare() {
        if (_isQueryParsed)
            return Status::OK();

        dassert(!_canonicalQuery.get());

        if (CanonicalQuery::isSimpleIdQuery(_request->getQuery())) {
            _isQueryParsed = true;
            return Status::OK();
        }

        CanonicalQuery* cqRaw;
        Status status = CanonicalQuery::canonicalize(_request->getNamespaceString().ns(),
                                                     _request->getQuery(),
                                                     &cqRaw);
        if (status.isOK()) {
            _canonicalQuery.reset(cqRaw);
            _isQueryParsed = true;
        }
        else if (status == ErrorCodes::NoClientContext) {
            // _isQueryParsed is still false, but execute() will try again under the lock.
            status = Status::OK();
        }
        return status;
    }

    long long DeleteExecutor::execute() {
        Collection *cl = getCollection(ns);
        if (cl == NULL) {
            return 0;
        }

        BSONObj obj;
        BSONObj pk = cl->getSimplePKFromQuery(pattern);

        // Fast-path for simple primary key deletes.
        if (!pk.isEmpty() && !cl->capped()) {
            if (queryByPKHack(cl, pk, pattern, obj)) {
                if (logop) {
                    OplogHelpers::logDelete(ns, obj, false);
                }
                deleteOneObject(cl, pk, obj);
                return 1;
            }
            return 0;
        }

        uassertStatusOK(prepare());
        uassert(0,
                mongoutils::str::stream() <<
                "DeleteExecutor::prepare() failed to parse query " << _request->getQuery(),
                _isQueryParsed);
        const bool logop = _request->shouldCallLogOp();
        const NamespaceString& ns(_request->getNamespaceString());
        if (!_request->isGod()) {
            if (ns.isSystem()) {
                uassert(12050,
                        "cannot delete from system namespace",
                        legalClientSystemNS(ns.ns(), true));
            }
            if (ns.ns().find('$') != string::npos) {
                log() << "cannot delete from collection with reserved $ in name: " << ns << endl;
                uasserted( 10100, "cannot delete from collection with reserved $ in name" );
            }
        }

        massert(0,
                mongoutils::str::stream() <<
                "dbname = " << cc()->database()->name() <<
                "; ns = " << ns.ns(),
                cc()->database()->name() == nsToDatabaseSubstring(ns.ns()));
        Collection *cl = getCollection(ns);
        if (cl == NULL) {
            return 0;
        }

        uassert(10101,
                str::stream() << "cannot remove from a capped collection: " << ns.ns(),
                !collection->capped());

        uassert(ErrorCodes::NotMaster,
                str::stream() << "Not primary while removing from " << ns.ns(),
                !logop || isMasterNs(ns.ns().c_str()));

        long long nDeleted = 0;
        for (shared_ptr<Cursor> c = getOptimizedCursor(ns, pattern); c->ok(); ) {
            pk = c->currPK();
            if (c->getsetdup(pk)) {
                c->advance();
                continue;
            }
            if (!c->currentMatches()) {
                c->advance();
                continue;
            }

            obj = c->current();

            // Non-multi deletes do not intend to advance, so there's
            // no reason to do so here and potentially overlock rows.
            if (!_request->isMulti()) {
                // There may be interleaved query plans that utilize multiple
                // cursors, some of which point to the same PK. We advance
                // here while those cursors point the row to be deleted.
                // 
                // Make sure to get local copies of pk/obj before advancing.
                pk = pk.getOwned();
                obj = obj.getOwned();
                while (c->ok() && c->currPK() == pk) {
                    c->advance();
                }
            }

            if (logop) {
                OplogHelpers::logDelete(ns, obj, false);
            }
            deleteOneObject(cl, pk, obj);
            nDeleted++;

            if (!_request->isMulti()) {
                break;
            }
        }
        return nDeleted;
    }

} // namespace mongo
